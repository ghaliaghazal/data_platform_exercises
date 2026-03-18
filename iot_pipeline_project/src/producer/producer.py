import json
import random
import time
from datetime import datetime
from kafka import KafkaProducer


# Koppla till Kafka
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

TOPIC = 'sensor_data_stream'

# # Simulerar en sensor som genererar data varje 2 sekunder
def generate_sensor_data():
    sensor_data = {
        "sensor_id": random.randint(1, 100),
        "temperature": round(random.uniform(20, 120), 2),
        "humidity": round(random.uniform(30, 90), 2),
        "vibration": round(random.uniform(0, 10), 2),
        "timestamp": datetime.utcnow().isoformat()
    }

    # 5% chans att skapa trasig data
    if random.random() < 0.05:
        sensor_data["temperature"] = "ERROR"

    return sensor_data


while True:
    data = generate_sensor_data()
    # Skicka data till Kafka
    producer.send(TOPIC, data)
    print(f"Skickade data: {data}")
    time.sleep(2)