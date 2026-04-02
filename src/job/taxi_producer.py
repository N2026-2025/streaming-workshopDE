import pandas as pd
import json
from time import time
from kafka import KafkaProducer
import numpy as np

# 1. Configuración del Productor con Timeout
try:
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        # Aumentamos el timeout por si Redpanda está lento
        request_timeout_ms=30000,
        retries=3
    )
    print("Conectado a Kafka exitosamente.")
except Exception as e:
    print(f"Error de conexión: {e}")
    exit()

# 2. Leer el archivo (IMPORTANTE: 2019-10 es el de la tarea)
url = 'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-10.parquet'
print("Descargando datos...")
df = pd.read_parquet(url)

# 3. Limpieza total de tipos de datos
# Kafka odia los tipos raros de numpy/pandas
df['lpep_pickup_datetime'] = df['lpep_pickup_datetime'].dt.strftime('%Y-%m-%d %H:%M:%S')
df['lpep_dropoff_datetime'] = df['lpep_dropoff_datetime'].dt.strftime('%Y-%m-%d %H:%M:%S')

# Reemplazar NaNs por 0 o None para que el JSON no explote
df = df.replace({np.nan: None})

columns = [
    'lpep_pickup_datetime', 'lpep_dropoff_datetime',
    'PULocationID', 'DOLocationID', 'passenger_count',
    'trip_distance', 'tip_amount', 'total_amount'
]
df = df[columns]

# 4. Envío con control de errores
t0 = time()
topic_name = 'green-trips'
count = 0

print(f"Enviando {len(df)} registros...")

for row in df.itertuples(index=False):
    try:
        # Convertir a dict y enviar
        message = row._asdict()
        producer.send(topic_name, value=message)
        count += 1
        
        # Opcional: imprimir cada 10000 para ver progreso
        if count % 10000 == 0:
            print(f"Enviados: {count} registros...")
            
    except Exception as e:
        print(f"Error enviando fila: {e}")
        break

producer.flush()
t1 = time()

print(f'¡Listo! Se enviaron {count} registros en {(t1 - t0):.2f} segundos.')
