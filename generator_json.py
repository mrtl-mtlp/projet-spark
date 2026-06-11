#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jun 11 06:34:21 2026

@author: cytech
"""

import json
import random
import time
from datetime import datetime, timezone

cities = ["Paris", "Lyon", "Marseille"]
categories = ["Vehicules", "Electronique", "Mobilier"]

actions = ["AIME", "VOUT", "ACHAT"]




users = {f"user_{i}" : random.choice(cities)
         for i in range(1,101)
         }



products = { f"product_{i}" :
           {
           "category" : random.choice(categories),
           "seller" : f"sel_{random.randint(1,100)}",
           "price" : round(random.uniform(50, 5000), 2)
           }
           for i in range(1,501)
    }

def now():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")



def stream():
    while True:

        user_id = random.choice(list(users.keys()))
        product_id = random.choice(list(products.keys()))

        user_city = users[user_id]
        product = products[product_id]

        event = {
            "timestamp": now(),

            "user_id": user_id,
            "user_city": user_city,

            "product_id": product_id,
            "product_cat": product["category"],

            "seller_id": product["seller"],
            "action_type": random.choice(actions),

            "price": product["price"]
        }

        yield event
  
   

gen = stream()

with open("stream_data/stream.json", "a") as f:
    while True:
        event = next(gen)
        f.write(json.dumps(event) + "\n")
        f.flush()
        print(event)
        time.sleep(1)



     

        



        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
