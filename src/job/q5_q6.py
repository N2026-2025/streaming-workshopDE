import os
from pyflink.table import EnvironmentSettings, TableEnvironment

def run_homework():
    # 1. Configuración para Docker
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = TableEnvironment.create(settings)

    # 2. Definición de la Tabla (Source)
    # IMPORTANTE: Dentro de Docker, usa 'redpanda:9092' o 'kafka:9092' 
    # en lugar de localhost si están en la misma red.
    source_ddl = """
        CREATE TABLE taxi_rides (
            PULocationID INT,
            lpep_pickup_datetime TIMESTAMP(3),
            tip_amount DOUBLE,
            WATERMARK FOR lpep_pickup_datetime AS lpep_pickup_datetime - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'green_taxi_rides',
            'properties.bootstrap.servers' = 'redpanda:9092',
            'properties.group.id' = 'flink-worker-v1',
            'scan.startup.mode' = 'earliest-offset',
            'format' = 'json'
        )
    """
    t_env.execute_sql(source_ddl)

    # 3. Pregunta 5: Session Window
    print("--- Calculando Q5: Sesión más larga ---")
    q5_sql = """
        SELECT 
            PULocationID, 
            COUNT(*) as trip_count
        FROM TABLE(
            SESSION(TABLE taxi_rides, DESCRIPTOR(lpep_pickup_datetime), INTERVAL '5' MINUTES)
        )
        GROUP BY PULocationID, window_start, window_end
        ORDER BY trip_count DESC
        LIMIT 1
    """
    # Usar .execute().print() para forzar la salida en consola
    t_env.execute_sql(q5_sql).print()

    # 4. Pregunta 6: Tumbling Window
    print("--- Calculando Q6: Hora con más propinas ---")
    q6_sql = """
        SELECT 
            window_start, 
            SUM(tip_amount) as total_tips
        FROM TABLE(
            TUMBLE(TABLE taxi_rides, DESCRIPTOR(lpep_pickup_datetime), INTERVAL '1' HOUR)
        )
        GROUP BY window_start, window_end
        ORDER BY total_tips DESC
        LIMIT 1
    """
    t_env.execute_sql(q6_sql).print()

if __name__ == '__main__':
    run_homework()
