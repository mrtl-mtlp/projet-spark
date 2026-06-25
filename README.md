# Projet Spark — Plateforme LeBonCoin-style

Petit projet qui simule un flux d'événements (likes, vues, achats) sur une plateforme de petites annonces, puis les analyse en temps réel avec Spark.

## Ce que fait le projet

1. **`generator_json.py`** — génère en continu de faux événements (un utilisateur qui aime, regarde ou achète un produit) et les écrit dans le dossier `stream_data/`.
2. **`script_principal.py`** — lit ce flux avec PySpark Structured Streaming, calcule des statistiques par fenêtre de temps (nombre d'actions, prix moyen) et construit un graphe (utilisateurs / vendeurs / produits) avec GraphFrames.
3. **`visualisation.py`** — petit serveur web (Flask) qui affiche un dashboard en direct dans le navigateur (`dashboard.html`).

## Prérequis

- Python 3.9 à 3.11 (PySpark 3.5 n'est pas garanti compatible avec Python 3.12+)
- Java 8, 11 ou 17 (obligatoire pour Spark)

Vérifier que Java est installé :
```bash
java -version
```
S'il n'est pas installé :
- **Linux (Debian/Ubuntu)** : `sudo apt install openjdk-17-jdk`
- **macOS** : `brew install openjdk@17`
- **Windows** : installer depuis [adoptium.net](https://adoptium.net/) (choisir la version 17)

## Installation

Sur les systèmes récents (Ubuntu 23.04+, Debian 12+, macOS avec Python géré par Homebrew...), `pip install` direct est bloqué par le système ("externally-managed-environment"). La solution qui fonctionne **partout** est de créer un environnement virtuel Python.

### 1. Se placer dans le dossier du projet

```bash
cd chemin/vers/projet-spark-main
```

### 2. Créer l'environnement virtuel

**Linux / macOS :**
```bash
python3 -m venv venv
source venv/bin/activate
```

**Windows (PowerShell) :**
```powershell
python -m venv venv
venv\Scripts\Activate.ps1
```

**Windows (cmd) :**
```cmd
python -m venv venv
venv\Scripts\activate.bat
```

Une fois activé, le prompt affiche `(venv)` au début de la ligne. **Il faut refaire `source venv/bin/activate` (ou l'équivalent Windows) à chaque nouvelle session de terminal.**

### 3. Installer les dépendances

Toujours avec le venv activé :
```bash
pip install "pyspark==3.5.*"
pip install flask
pip install graphframes-py
```


## Lancer le projet

### Option A — tout en une commande (Linux/macOS)

```bash
./run.sh
```

Si le script n'est pas exécutable :
```bash
chmod +x run.sh
./run.sh
```

Le script détecte automatiquement le venv local (`./venv`) et lance dans l'ordre :
1. le générateur de flux,
2. le pipeline Spark,
3. le dashboard (ouvert automatiquement dans le navigateur).

Changer la fréquence de rafraîchissement du dashboard (en secondes) :
```bash
./run.sh 2
```

Pour tout arrêter : `Ctrl + C`.

### Option B — lancer chaque programme à la main (toutes plateformes, y compris Windows)

Dans 3 terminaux différents, avec le venv activé dans chacun :

```bash
python generator_json.py
```
```bash
python script_principal.py
```
```bash
python visualisation.py
```

Le dashboard est ensuite accessible sur **http://localhost:5000**.


## Structure du projet

```
.
├── generator_json.py     # génère les événements
├── script_principal.py   # pipeline Spark + GraphFrames
├── visualisation.py      # serveur Flask pour le dashboard
├── dashboard.html         # page web du dashboard
├── run.sh                 # lance tout en une commande (Linux/macOS)
├── requirements.txt       # dépendances Python
└── stream_data/           # événements générés (fichiers JSON)
```
