#!/usr/bin/env python3
"""Générateur de flux infini d'événements LeBonCoin-style."""

import json
import random
import time
from datetime import datetime, timezone
from pathlib import Path
import uuid




cities      = ["Paris", "Lyon", "Marseille", "Bordeaux", "Toulouse"]
categories  = ["Vehicules", "Electronique", "Mobilier", "Vetements", "Loisirs"]
actions     = ["AIME", "VOUT", "ACHAT"]



users    = {f"user_{i}":    random.choice(cities) for i in range(1, 101)}
products = {f"product_{i}": 
            {
                "category": random.choice(categories),
                "seller":   f"sel_{random.randint(1, 20)}",
                "price":    round(random.uniform(10, 5000), 2)
                }
            for i in range(1, 501)}




def stream():
    while True:
        uid  = random.choice(list(users))
        pid  = random.choice(list(products))
        prod = products[pid]
        event = {
            "timestamp":   datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "user_id":     uid,
            "user_city":   users[uid],
            "product_id":  pid,
            "product_cat": prod["category"],
            "seller_id":   prod["seller"],
            "action_type": random.choices(actions, weights=[6, 3, 1], k=1)[0],
            "price":       prod["price"],
        }
        
        yield event



gen = stream()

Path("stream_data").mkdir(exist_ok=True)
while True:
    event = next(gen)
    # Création d'un nom de fichier unique pour chaque événement
    filename = f"stream_data/event_{uuid.uuid4()}.json"
    with open(filename, "w") as f:
        f.write(json.dumps(event) + "\n")
    print(f"Événement écrit : {filename}")
    time.sleep(1)
        
