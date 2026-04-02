from pyflink.table import EnvironmentSettings, TableEnvironment
from pyflink.datastream import StreamExecutionEnvironment

# 1. Configurar entorno
env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(1)

settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
t_env = TableEnvironment.create(settings)

# 2. Source Kafka
source_ddl = """
CREATE TABLE green_trips (
    lpep_pickup_datetime VARCHAR,
    lpep_dropoff_datetime VARCHAR,
    PULocationID INT,
    DOLocationID INT,
    passenger_count DOUBLE,
    trip_distance DOUBLE,
    tip_amount DOUBLE,
    total_amount DOUBLE,

    event_timestamp AS TO_TIMESTAMP(lpep_pickup_datetime, 'yyyy-MM-dd HH:mm:ss'),
    WATERMARK FOR event_timestamp AS event_timestamp - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'green-trips',
    'properties.bootstrap.servers' = 'redpanda:9092',
    'properties.group.id' = 'flink-tip',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json'
)
"""

t_env.execute_sql(source_ddl)

# 3. Sink PostgreSQL
sink_ddl = """
CREATE TABLE tip_hourly (
    window_start TIMESTAMP,
    total_tip DOUBLE,
    PRIMARY KEY (window_start) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres:5432/postgres',
    'table-name' = 'tip_hourly',
    'username' = 'postgres',
    'password' = 'postgres',
    'driver' = 'org.postgresql.Driver'
)
"""

t_env.execute_sql(sink_ddl)

# 4. Query
query = """
INSERT INTO tip_hourly
SELECT
    window_start,
    SUM(tip_amount) as total_tip
FROM TABLE(
    TUMBLE(
        TABLE green_trips,
        DESCRIPTOR(event_timestamp),
        INTERVAL '1' HOUR
    )
)
GROUP BY window_start
"""

t_env.execute_sql(query)