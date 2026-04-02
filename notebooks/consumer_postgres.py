from kafka import KafkaConsumer
from models import ride_deserializer # <-- ¡No olvides esto!

import psycopg2
from datetime import datetime
from models import ride_deserializer # Asegúrate de que esto esté bien importado

server = 'localhost:9092'
topic_name = 'rides'

consumer = KafkaConsumer(
    topic_name,
    bootstrap_servers=[server],
    auto_offset_reset='earliest',
    group_id='rides-to-postgres',
    value_deserializer=ride_deserializer,
    api_version=(0, 10, 1) # <-- Añade esto para estar seguro
)



# 1. Conectamos a Postgres (Asegúrate de que los datos coincidan con tu docker-compose)
conn = psycopg2.connect("host=localhost dbname=postgres user=postgres password=postgres")
conn.autocommit = True
cur = conn.cursor()

print(f"Escuchando {topic_name} y escribiendo en PostgreSQL...")

count = 0
try:
    for message in consumer:
        ride = message.value
        # Convertimos milisegundos a fecha de Python
        pickup_dt = datetime.fromtimestamp(ride.tpep_pickup_datetime / 1000)
        
        # Insertamos en la tabla
        cur.execute(
            """INSERT INTO processed_events
               (PULocationID, DOLocationID, trip_distance, total_amount, pickup_datetime)
               VALUES (%s, %s, %s, %s, %s)""",
            (ride.PULocationID, ride.DOLocationID,
             ride.trip_distance, ride.total_amount, pickup_dt)
        )
        
        count += 1
        if count % 100 == 0:
            print(f"¡Insertadas {count} filas hasta ahora!")

except KeyboardInterrupt:
    print("Detenido por el usuario.")
finally:
    # Cerramos todo al terminar
    consumer.close()
    cur.close()
    conn.close()
