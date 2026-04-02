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
