import json
import time
import random
from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda v: json.dumps(v).encode('utf-8'))

real_users = ["A2EFCYXHNK06IS", "A1WR23ER5HMAA9", "A2IR4Q0GPAFJKW", "A2V0KUVAB9HSYO", "A1J0GL9HCA7ELW", "A3EBHHCHO6V2A4", "A340XJYJDFSMUG", "A3Q1J7VFGG80EK"]
real_items = ["5555991584", "B000002URV", "B000002URW", "B000002URX", "B000002URY"]

while True:
    data = {
        "user": random.choice(real_users),
        "item": random.choice(real_items),
        "rating": round(random.uniform(3.0, 5.0), 1),
        "timestamp": int(time.time())
    }
    producer.send("music_events", value=data)
    time.sleep(0.5)
