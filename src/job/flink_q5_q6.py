import os
from pyflink.table import EnvironmentSettings, TableEnvironment

def run_homework():
    # 1. Configurar entorno
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = TableEnvironment.create(settings)

    # 2. Cargar el JAR del conector (Asegurate que el nombre coincida con el descargado)
    # Buscamos cualquier jar de kafka en la carpeta actual
    jar_path = f"file://{os.getcwd()}/flink-sql-connector-kafka-3.0.1-1.18.jar"
    t_env.get_config().set("pipeline.jars", jar_path)

    # 3. Crear la tabla de origen (Source)
    # IMPORTANTE: El tópico debe ser 'green-trips' (como tu productor)
    t_env.execute_sql("""
        CREATE TABLE taxi_rides (
            PULocationID INT,
            lpep_pickup_datetime TIMESTAMP(3),
            tip_amount DOUBLE,
            WATERMARK FOR lpep_pickup_datetime AS lpep_pickup_datetime - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'green-trips',
            'properties.bootstrap.servers' = 'localhost:9092',
            'properties.group.id' = 'test-group',
            'scan.startup.mode' = 'earliest-offset',
            'format' = 'json'
        )
    """)

    print("--- Calculando Pregunta 5 (Sesión de 5 min) ---")
    # Usamos SQL para evitar errores de sintaxis de la Table API
    t_env.execute_sql("""
        SELECT 
            PULocationID, 
            COUNT(*) as trip_count
        FROM TABLE(
            SESSION(TABLE taxi_rides, DESCRIPTOR(lpep_pickup_datetime), INTERVAL '5' MINUTES)
        )
        GROUP BY PULocationID, window_start, window_end
        ORDER BY trip_count DESC
        LIMIT 1
    """).print()

    print("\n--- Calculando Pregunta 6 (Tumbling 1 hora) ---")
    t_env.execute_sql("""
        SELECT 
            window_start, 
            SUM(tip_amount) as total_tips
        FROM TABLE(
            TUMBLE(TABLE taxi_rides, DESCRIPTOR(lpep_pickup_datetime), INTERVAL '1' HOUR)
        )
        GROUP BY window_start, window_end
        ORDER BY total_tips DESC
        LIMIT 1
    """).print()

if __name__ == '__main__':
    run_homework()
