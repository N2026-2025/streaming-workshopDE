from pyflink.table import EnvironmentSettings, TableEnvironment
from pyflink.datastream import StreamExecutionEnvironment

env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(1)

settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
t_env = TableEnvironment.create(settings)

# Source: Kafka (Redpanda)

source_ddl = """
CREATE TABLE green_trips (
    lpep_pickup_datetime VARCHAR,
    lpep_dropoff_datetime VARCHAR,
    PULocationID INT,
    DOLocationID INT,
    passenger_count INT,
    trip_distance DOUBLE,
    tip_amount DOUBLE,
    total_amount DOUBLE,

    event_timestamp AS TO_TIMESTAMP(lpep_pickup_datetime, 'yyyy-MM-dd HH:mm:ss'),
    WATERMARK FOR event_timestamp AS event_timestamp - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'green-trips',
    'properties.bootstrap.servers' = 'redpanda:9092',
    'properties.group.id' = 'green-consumer',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json'
)
"""

t_env.execute_sql(source_ddl)


# Sink PostgreSQL

sink_ddl = """
CREATE TABLE trip_stats (
    window_start TIMESTAMP,
    window_end TIMESTAMP,
    trip_count BIGINT
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres:5432/postgres',
    'table-name' = 'trip_stats',
    'username' = 'postgres',
    'password' = 'postgres',
    'driver' = 'org.postgresql.Driver'
)
"""

t_env.execute_sql(sink_ddl)


# Window aggregation

query = """
INSERT INTO trip_stats
SELECT
    window_start,
    window_end,
    COUNT(*)
FROM TABLE(
    TUMBLE(
        TABLE green_trips,
        DESCRIPTOR(event_timestamp),
        INTERVAL '1' MINUTE
    )
)
GROUP BY window_start, window_end
"""

t_env.execute_sql(query)