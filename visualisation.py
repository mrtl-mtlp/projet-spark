#!/usr/bin/env python3
"""
Serveur Flask + SSE — Dashboard graphe LeBonCoin

"""

import json, time, threading, webbrowser
from pathlib import Path
from flask import Flask, Response

app = Flask(__name__)
STREAM_DIR = Path("stream_data")

# Ensemble des fichiers déjà lus (en mémoire, pas de suppression)
seen = set()
listeners = []
listeners_lock = threading.Lock()

def broadcast(data):
    with listeners_lock:
        dead = []
        for q in listeners:
            try:
                q.put_nowait(data)
            except Exception:
                dead.append(q)
        for q in dead:
            listeners.remove(q)

def watcher():
    """Scrute stream_data/ toutes les 0.5s, envoie les nouveaux fichiers."""
    import queue
    while True:
        for f in sorted(STREAM_DIR.glob("*.json")):
            if f.name not in seen:
                seen.add(f.name)
                try:
                    broadcast(f.read_text())
                except Exception:
                    pass
        time.sleep(0.5)

threading.Thread(target=watcher, daemon=True).start()

# ── SSE ──────────────────────────────────────────────────────────────────────

@app.route("/events")
def events():
    import queue
    q = queue.Queue(maxsize=200)
    with listeners_lock:
        listeners.append(q)
    def stream():
        yield "data: {}\n\n"
        while True:
            try:
                yield f"data: {q.get(timeout=30)}\n\n"
            except Exception:
                yield ": ping\n\n"
    return Response(stream(), mimetype="text/event-stream",
                    headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


@app.route("/")
def index():
    return open("dashboard.html").read()


if __name__ == "__main__":
    port = 5000
    # Fonction pour ouvrir le navigateur après un court délai
    def open_browser():
        time.sleep(1.5)  # Attend que le serveur Flask soit bien démarré
        webbrowser.open(f"http://localhost:{port}")

    threading.Thread(target=open_browser, daemon=True).start()
    
    print(f"→ http://localhost:{port}")
    app.run(host="0.0.0.0", port=port, threaded=True)
    
    