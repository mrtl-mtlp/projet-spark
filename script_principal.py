#!/usr/bin/env python3
"""
Script principal qui trace les graphes

"""

import json
import os
import sys
import tempfile
from datetime import datetime, timezone

# ── 0. Bootstrap environnement (avant toute initialisation Spark) ────────────
# Les workers Spark doivent utiliser EXACTEMENT le même interpréteur Python que
# le driver, sinon ils retombent sur un python système incompatible.
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

# Spark 4.x requiert Java 17 ou 21 (Java 24+ casse Hadoop : getSubject supprimé).
# On bascule automatiquement sur un JDK compatible installé via Homebrew.
if "JAVA_HOME" not in os.environ:
    for _jh in ("/opt/homebrew/opt/openjdk@17", "/opt/homebrew/opt/openjdk@21",
                "/usr/local/opt/openjdk@17", "/usr/local/opt/openjdk@21"):
        if os.path.isdir(_jh):
            os.environ["JAVA_HOME"] = _jh
            break

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (StructType, StructField, StringType, DoubleType, TimestampType)
from graphframes import GraphFrame


# Dossier où chaque micro-batch dépose l'état courant du graphe (lu par visualisation.py)
SNAPSHOT_PATH = "graph_snapshot/graph.json"



# ── 1. Session Spark ────────────────────────────────────────────────────────

spark = (SparkSession.builder
    .appName("LeBonCoin_Streaming")
    .config("spark.sql.shuffle.partitions", "4")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .config("spark.jars.packages", "graphframes:graphframes:0.8.3-spark3.5-s_2.12")
    .getOrCreate())

spark.sparkContext.setLogLevel("WARN")





# ── 2. Schéma strict (pas d'inférence) ──────────────────────────────────────

schema = StructType([
    StructField("timestamp",   TimestampType(), False),
    StructField("user_id",     StringType(),    False),
    StructField("user_city",   StringType(),    True),
    StructField("product_id",  StringType(),    False),
    StructField("product_cat", StringType(),    True),
    StructField("seller_id",   StringType(),    False),
    StructField("action_type", StringType(),    False),
    StructField("price",       DoubleType(),    True),
])





# ── 3. Lecture du flux ───────────────────────────────────────────────────────

raw = spark.readStream.format("json").schema(schema).load("stream_data")



# ── 4. Watermark + fenêtres ──────────────────────────────────────────────────

watermarked = raw.withWatermark("timestamp", "30 seconds")

# Fenêtre glissante : volume d'actions par tranche de 60s, glissement de 10s
action_window = (watermarked
    .groupBy(F.window("timestamp", "60 seconds", "10 seconds"), "action_type")
    .agg(F.count("*").alias("count"), F.avg("price").alias("avg_price")))







# ── 5. foreachBatch : graphe GraphFrames ─────────────────────────────────────

def export_graph_snapshot(g, batch_id):
    """Sérialise l'état courant du graphe (sommets typés + degrés et arêtes
    typées/pondérées) dans un fichier JSON unique, écrasé à chaque micro-batch.
    C'est ce fichier que l'interface de visualisation (visualisation.py) relit
    périodiquement pour rafraîchir l'affichage (cf. spec 2.2)."""

    # Degré total de chaque sommet → traduit l'évolution de la centralité
    degrees = {row["id"]: row["degree"] for row in g.degrees.collect()}

    nodes = [
        {"id": row["id"], "type": row["type"], "degree": degrees.get(row["id"], 0)}
        for row in g.vertices.collect()
    ]

    # Arêtes orientées et pondérées : poids = nombre d'interactions du même type
    edges = [
        {"src": row["src"], "dst": row["dst"],
         "relationship": row["relationship"], "weight": row["weight"]}
        for row in (g.edges
                    .groupBy("src", "dst", "relationship")
                    .agg(F.count("*").alias("weight"))
                    .collect())
    ]

    snapshot = {
        "batch_id": batch_id,
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "nodes": nodes,
        "edges": edges,
    }

    # Écriture atomique (temp + rename) pour que le lecteur ne voie jamais un
    # fichier à moitié écrit pendant son rafraîchissement.
    os.makedirs(os.path.dirname(SNAPSHOT_PATH), exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=os.path.dirname(SNAPSHOT_PATH), suffix=".tmp")
    with os.fdopen(fd, "w") as f:
        json.dump(snapshot, f)
    os.replace(tmp, SNAPSHOT_PATH)


def process_batch(batch_df, batch_id):
    if batch_df.rdd.isEmpty():
        return

    # Sommets : Utilisateurs (U), Vendeurs (S), Produits (P)
    vertices = (
        batch_df.select(F.col("user_id").alias("id"),    F.lit("U").alias("type"))
        .union(batch_df.select(F.col("seller_id").alias("id"), F.lit("S").alias("type")))
        .union(batch_df.select(F.col("product_id").alias("id"), F.lit("P").alias("type")))
        .dropDuplicates(["id"])
    )

    # Arêtes : user→product et seller→product
    edges = (
        batch_df.select(F.col("user_id").alias("src"),
                        F.col("product_id").alias("dst"),
                        F.col("action_type").alias("relationship"))
        .union(batch_df.select(F.col("seller_id").alias("src"),
                               F.col("product_id").alias("dst"),
                               F.lit("PROPOSE").alias("relationship")))
    )

    g = GraphFrame(vertices, edges)

    print(f"\n── Batch {batch_id} ──")
    print(f"  Sommets : {g.vertices.count()}  |  Arêtes : {g.edges.count()}")
    print("  Top in-degrees :")
    g.inDegrees.orderBy(F.desc("inDegree")).show(5, truncate=False)

    # Rafraîchissement de la vue graphique : dépôt du snapshot courant (2.2)
    export_graph_snapshot(g, batch_id)






# ── 6. Écriture des requêtes ─────────────────────────────────────────────────

# Requête 1 — agrégations fenêtrées (mode Update, console)
q1 = (action_window.writeStream
    .outputMode("update")


    .format("console")
    .option("truncate", False)
    .trigger(processingTime="5 seconds")
    .option("checkpointLocation", "/tmp/chk/actions")
    .start())

# Requête 2 — graphe par micro-batch (mode Append, foreachBatch)
q2 = (raw.writeStream
    .outputMode("append")
    .foreachBatch(process_batch)
    .trigger(processingTime="5 seconds")
    .option("checkpointLocation", "/tmp/chk/graph")
    .start())







# ── 7. Attente infinie ───────────────────────────────────────────────────────

spark.streams.awaitAnyTermination()








