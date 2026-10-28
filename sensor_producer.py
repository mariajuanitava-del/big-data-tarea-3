import json
import random
import time
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)
try:
    while True:
        data ={
            "sensor_id": random.randint(1, 10),
            "temperature": random.uniform(20, 30),
            "humidity": random.uniform(30, 70),
            "timestamp": int(time.time())
        }
        producer.send("sensor_data", data).get(timeout=10)
        print(data, flush=True)
        time.sleep(1)
except KeyboardInterrupt:
    print("envio detenido")
finally:
    producer.close()


