WITH active_users_7d AS (
    SELECT COUNT(DISTINCT ds.user_id) AS cnt
    FROM dds.fact_session fs
    JOIN dds.dim_session ds ON ds.id = fs.session_dim_id
    WHERE fs.session_start_time >= now() - interval '7 days'
)
SELECT
    du.user_id,
    COUNT(fs.session_dim_id)::BIGINT AS sessions_count,
    ROUND(
        AVG(EXTRACT(EPOCH FROM (fs.session_end_time - fs.session_start_time)) / 60)::NUMERIC,
        2
    ) AS avg_session_duration,
    ROUND(
        CASE WHEN COUNT(fs.session_dim_id) > 0 THEN SUM(fs.pages_viewed)::NUMERIC / COUNT(fs.session_dim_id) ELSE 0 END,
        2
    ) AS avg_pages_per_session,
    COUNT(fe.event_id)::BIGINT AS events_count,
    COALESCE(au7.cnt, 0)::BIGINT AS active_users_7d,
    EXTRACT(EPOCH FROM now())::BIGINT AS version
FROM dds.dim_user du
LEFT JOIN dds.dim_session ds ON ds.user_id = du.id
LEFT JOIN dds.fact_session fs ON fs.session_dim_id = ds.id
LEFT JOIN dds.fact_event fe ON fe.session_id = fs.session_dim_id
CROSS JOIN active_users_7d au7
GROUP BY du.user_id, au7.cnt
ORDER BY du.user_id;
