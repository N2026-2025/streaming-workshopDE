from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'green-trips',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    consumer_timeout_ms=5000,
    value_deserializer=lambda x: json.loads(x.decode())
)

count = 0

for message in consumer:
    trip = message.value   # ya es dict

    if float(trip["trip_distance"]) > 5:
        count += 1

print("Trips > 5km:", count)