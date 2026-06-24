#!/usr/bin/env python3
"""
Interface graphique de rafraîchissement dynamique du graphe de connexions (2.2).

Lit en continu le snapshot déposé par le pipeline PySpark (`script_principal.py`,
fonction `export_graph_snapshot`) et redessine le graphe à intervalle
paramétrable. Chaque rafraîchissement reflète l'évolution des degrés des nœuds
et les nouvelles connexions tissées par le flux.

  Nœuds (sommets)  : Utilisateurs (U), Vendeurs (S), Produits (P)
                     → couleur ET forme distinctes, taille ∝ degré.
  Liens (arêtes)   : interactions orientées et typées (AIME / VOUT / ACHAT /
                     PROPOSE), épaisseur ∝ poids (nombre d'interactions).

Lancement :
    python3 visualisation.py            # rafraîchit toutes les 5 s
    python3 visualisation.py 2          # rafraîchit toutes les 2 s
"""

import json
import os
import sys

import matplotlib.pyplot as plt
import networkx as nx


# ── Paramètres ───────────────────────────────────────────────────────────────

SNAPSHOT_PATH   = "graph_snapshot/graph.json"          # produit par le pipeline Spark
REFRESH_SECONDS = float(sys.argv[1]) if len(sys.argv) > 1 else 5.0   # fréquence paramétrable
MAX_EDGES       = 45          # plafond d'arêtes affichées (les + fortes) pour rester lisible

# Style par type d'entité : (couleur, forme matplotlib, libellé de légende)
NODE_STYLE = {
    "U": ("#4C9BE8", "o", "Utilisateur (U)"),   # bleu  / cercle
    "S": ("#E8554C", "s", "Vendeur (S)"),        # rouge / carré
    "P": ("#2BB673", "^", "Produit (P)"),        # vert  / triangle
}


# ── Lecture du snapshot ──────────────────────────────────────────────────────

def load_snapshot():
    """Relit le dernier état du graphe. Renvoie None si rien n'est encore prêt."""
    if not os.path.exists(SNAPSHOT_PATH):
        return None
    try:
        with open(SNAPSHOT_PATH) as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError):
        # Snapshot en cours d'écriture : on réessaiera au prochain tour.
        return None


def trim_for_readability(nodes, edges):
    """Garde au plus MAX_EDGES connexions (les + fortes) + les seuls nœuds qu'elles
    relient. On filtre par arête (et non par nœud) car le graphe est biparti
    U/S → P : enlever un produit isolerait toutes ses arêtes. Le plafond est
    réparti moitié/moitié entre arêtes utilisateur (AIME/VOUT/ACHAT) et arêtes
    vendeur (PROPOSE) pour que les trois types de nœuds restent visibles."""
    if len(edges) > MAX_EDGES:
        half = MAX_EDGES // 2
        user_e = sorted([e for e in edges if e["relationship"] != "PROPOSE"],
                        key=lambda e: e["weight"], reverse=True)[:half]
        sell_e = sorted([e for e in edges if e["relationship"] == "PROPOSE"],
                        key=lambda e: e["weight"], reverse=True)[:MAX_EDGES - half]
        edges = user_e + sell_e
    kept_ids = {e["src"] for e in edges} | {e["dst"] for e in edges}
    nodes = [n for n in nodes if n["id"] in kept_ids]
    return nodes, edges


# ── Dessin ───────────────────────────────────────────────────────────────────

def redraw(ax, snapshot, pos_cache):
    ax.clear()

    nodes, edges = trim_for_readability(snapshot["nodes"], snapshot["edges"])

    G = nx.DiGraph()
    for n in nodes:
        G.add_node(n["id"], type=n["type"], degree=n["degree"])
    # Arêtes typées/pondérées (fusion des éventuels doublons src→dst).
    for e in edges:
        if G.has_edge(e["src"], e["dst"]):
            d = G[e["src"]][e["dst"]]
            d["weight"] += e["weight"]
            if e["relationship"] not in d["label"]:
                d["label"] += "/" + e["relationship"]
        else:
            G.add_edge(e["src"], e["dst"], weight=e["weight"], label=e["relationship"])

    if G.number_of_nodes() == 0:
        ax.set_title("En attente d'événements du flux…")
        ax.axis("off")
        return

    # Positions stables : on réutilise les positions connues d'un rafraîchissement
    # à l'autre, et on n'(re)calcule que pour les nouveaux nœuds.
    fixed = {nid: pos_cache[nid] for nid in G.nodes() if nid in pos_cache}
    pos = nx.spring_layout(
        G, k=0.9, seed=42, iterations=60,
        pos=fixed or None,
        fixed=list(fixed) if fixed else None,
    )
    pos_cache.clear()
    pos_cache.update(pos)

    # Arêtes orientées (épaisseur ∝ poids) + étiquettes de type.
    weights = [G[u][v]["weight"] for u, v in G.edges()]
    nx.draw_networkx_edges(
        G, pos, ax=ax, edge_color="#9aa0a6", arrows=True,
        arrowstyle="-|>", arrowsize=14, width=[1 + w for w in weights],
        connectionstyle="arc3,rad=0.05",
    )
    nx.draw_networkx_edge_labels(
        G, pos, ax=ax, font_size=7, font_color="#5f6368",
        edge_labels={(u, v): G[u][v]["label"] for u, v in G.edges()},
        label_pos=0.5, rotate=False,
    )

    # Sommets : un appel par type pour différencier couleur ET forme.
    for ntype, (color, shape, _) in NODE_STYLE.items():
        ids = [n for n, d in G.nodes(data=True) if d["type"] == ntype]
        if not ids:
            continue
        # taille ∝ degré, mais plafonnée pour éviter des marqueurs géants qui se chevauchent
        sizes = [min(140 + 45 * G.nodes[n]["degree"], 800) for n in ids]
        nx.draw_networkx_nodes(
            G, pos, ax=ax, nodelist=ids, node_color=color,
            node_shape=shape, node_size=sizes, edgecolors="white", linewidths=1.0,
        )
    nx.draw_networkx_labels(G, pos, ax=ax, font_size=7, font_color="#202124")

    # Légende + titre récapitulatif.
    handles = [
        plt.Line2D([0], [0], marker=shape, color="w", markerfacecolor=color,
                   markersize=11, label=label)
        for color, shape, label in NODE_STYLE.values()
    ]
    ax.legend(handles=handles, loc="upper left", fontsize=8, framealpha=0.9)
    ax.set_title(
        f"Graphe de connexions — batch {snapshot['batch_id']} "
        f"({len(G.nodes())} sommets, {len(G.edges())} arêtes) — "
        f"maj {snapshot['generated_at']}  •  refresh {REFRESH_SECONDS:g}s",
        fontsize=10,
    )
    ax.axis("off")


# ── Boucle de rafraîchissement ───────────────────────────────────────────────

def main():
    plt.ion()
    fig, ax = plt.subplots(figsize=(12, 8))
    fig.canvas.manager.set_window_title("LeBonCoin — Graphe temps réel")
    pos_cache = {}

    print(f"Visualisation lancée (refresh = {REFRESH_SECONDS:g}s).")
    print(f"Lecture du snapshot : {SNAPSHOT_PATH}")
    print("Fermez la fenêtre pour arrêter.")

    while plt.fignum_exists(fig.number):
        snapshot = load_snapshot()
        if snapshot is not None:
            redraw(ax, snapshot, pos_cache)
        else:
            ax.clear()
            ax.set_title("En attente du premier micro-batch Spark…")
            ax.axis("off")
        fig.canvas.draw_idle()
        plt.pause(REFRESH_SECONDS)


if __name__ == "__main__":
    main()
