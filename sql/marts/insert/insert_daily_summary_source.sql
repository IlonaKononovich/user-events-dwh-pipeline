SELECT
  fo.event_time::date AS event_date,
  SUM(fo.total_amount) AS revenue,
  COUNT(DISTINCT fo.order_id) AS orders_count,
  COUNT(DISTINCT fo.user_id) AS unique_users,
  ROUND(AVG(fo.total_amount), 2) AS avg_check,
  COUNT(DISTINCT fo.user_id)::float / NULLIF(COUNT(DISTINCT fs.session_dim_id), 0) AS conversion_rate,
  COUNT(DISTINCT CASE 
    WHEN ds.start_time::date = fo.event_time::date THEN fo.user_id 
  END) AS new_users,
  EXTRACT(EPOCH FROM now())::bigint AS version
FROM dds.fact_order fo
JOIN dds.fact_session fs ON fs.session_dim_id = fo.session_id
JOIN dds.dim_session ds ON ds.id = fs.session_dim_id
GROUP BY event_date
ORDER BY event_date
