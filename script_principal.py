#!/usr/bin/env python3
"""
Pipeline PySpark Structured Streaming + GraphFrames
Plateforme d'interactions LeBonCoin-style.

"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (StructType, StructField, StringType, DoubleType, TimestampType)
from graphframes import GraphFrame



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








