from pyflink.table import EnvironmentSettings, TableEnvironment
from pyflink.table.window import Session

env_settings = EnvironmentSettings.in_streaming_mode()
t_env = TableEnvironment.create(env_settings)

# Assume source_table created from Kafka/green-taxi or similar
t_env.execute_sql("""
CREATE TABLE source_table (
    lpep_pickup_datetime TIMESTAMP(3),
    PULocationID INT,
    -- other fields
) WITH (
    'connector' = 'kafka',
    -- ...
)
""")

result = t_env.sql_query("""
SELECT 
    PULocationID,
    SESSION_TIME AS session_start,
    COUNT(*) AS trip_count
FROM TABLE(
    SESSION(
        TABLE source_table,
        DESCRIPTOR(`lpep_pickup_datetime`),
        INTERVAL '5' MINUTES
    )
)
GROUP BY PULocationID, SESSION_TIME
""")

# Write to PostgreSQL sink, then query MAX(trip_count)
result.execute_insert("postgres_sink")

tip_result = t_env.sql_query("""
SELECT 
    window_start,
    window_end,
    SUM(tip_amount) AS total_tip
FROM TABLE(
    TUMBLE(
        TABLE source_table,
        DESCRIPTOR(`lpep_pickup_datetime`),
        INTERVAL '1' HOURS
    )
)
GROUP BY window_start, window_end
""")

tip_result.execute_insert("postgres_tip_sink")
