#!/usr/bin/env bash
#
# Lance tout le projet en une seule commande :
#   1. le générateur de flux infini (generator_json.py)
#   2. le pipeline PySpark + GraphFrames (script_principal.py)
#   3. le dashboard graphique qui se rafraîchit (visualisation.py)
#
# Usage :  ./run.sh [intervalle_refresh_secondes]
#          ./run.sh           # rafraîchit toutes les 5 s
#          ./run.sh 2         # rafraîchit toutes les 2 s
#
# Ctrl+C arrête proprement les trois processus.
 
cd "$(dirname "$0")" || exit 1
 
# Interpréteur Python qui contient pyspark 3.5 (un script non-interactif ne voit
# pas l'alias `python3` du shell : on choisit explicitement le bon binaire).
# On regarde d'abord si un environnement virtuel local existe (./venv ou ./.venv).
PY=""
for cand in ./venv/bin/python3 ./.venv/bin/python3 /opt/homebrew/opt/python@3.11/bin/python3.11 python3.11 python3; do
    if command -v "$cand" >/dev/null 2>&1 && "$cand" -c "import pyspark" >/dev/null 2>&1; then
        PY="$cand"; break
    fi
done
if [ -z "$PY" ]; then
    echo "Erreur : aucun Python avec pyspark trouvé." >&2
    echo "→ Crée un environnement virtuel et installe les dépendances :" >&2
    echo "    python3 -m venv venv" >&2
    echo "    source venv/bin/activate" >&2
    echo "    pip install "pyspark==3.5.*"" >&2
    echo "    pip install flask" >&2
    echo "    pip install graphframes-py" >&2
    exit 1
fi
echo "Interpréteur : $PY"

# Arrêt propre de tout le groupe à Ctrl+C.
trap 'echo; echo "Arrêt…"; kill 0 2>/dev/null' EXIT

echo "1/3  Générateur de flux…"
"$PY" generator_json.py  > /tmp/generator.log 2>&1 &

echo "2/3  Pipeline PySpark…  (logs : /tmp/pipeline.log)"
"$PY" script_principal.py > /tmp/pipeline.log 2>&1 &

echo "3/3  Dashboard graphique (fenêtre)…"
"$PY" visualisation.py "${1:-5}"
