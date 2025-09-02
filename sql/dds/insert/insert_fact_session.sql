INSERT INTO dds.fact_session (
    session_dim_id,
    date_id,
    session_start_time,
    session_end_time,
    pages_viewed,
    events_count,
    is_converted
)
SELECT
    s.id AS session_dim_id,
    dt.id AS date_id,
    s.start_time AS session_start_time,
    s.end_time AS session_end_time,
    MAX(r.pages_viewed) AS pages_viewed,
    COUNT(r.event_id) AS events_count,
    BOOL_OR(r.event_type = 'purchase') AS is_converted
FROM staging.events r
JOIN dds.dim_session s ON s.session_id = r.session_id
JOIN dds.dim_date dt ON dt.date = DATE(s.start_time)
LEFT JOIN dds.fact_session fs ON fs.session_dim_id = s.id
WHERE fs.id IS NULL
GROUP BY s.id, dt.id, s.start_time, s.end_time;