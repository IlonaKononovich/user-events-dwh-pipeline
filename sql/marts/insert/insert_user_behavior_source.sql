SELECT
  du.user_id,
  COUNT(fs.session_dim_id) AS sessions_count,
  ROUND(AVG(EXTRACT(EPOCH FROM (fs.session_end_time - fs.session_start_time)) / 60)::numeric, 2) AS avg_session_duration,
  SUM(fs.pages_viewed) AS pages_viewed,
  COUNT(DISTINCT fs.session_dim_id)::float / NULLIF(COUNT(DISTINCT first_sessions.first_session_id), 0) AS retention_rate,
  EXTRACT(EPOCH FROM now())::bigint AS version
FROM dds.fact_session fs
JOIN dds.dim_session ds ON ds.id = fs.session_dim_id
JOIN dds.dim_user du ON du.id = ds.user_id
LEFT JOIN (
  SELECT
    ds.user_id,
    MIN(fs.session_dim_id) AS first_session_id
  FROM dds.fact_session fs
  JOIN dds.dim_session ds ON ds.id = fs.session_dim_id
  GROUP BY ds.user_id
) first_sessions ON first_sessions.user_id = du.id
GROUP BY du.user_id
ORDER BY du.user_id
