WITH date_base AS (
    SELECT dd.date AS event_date
    FROM dds.dim_date dd
),
orders AS (
    SELECT
        dd.date AS event_date,
        SUM(fo.total_amount) AS revenue,
        COUNT(DISTINCT fo.order_id) AS orders_count,
        COUNT(DISTINCT du.user_id) AS unique_users,
        ROUND(SUM(fo.total_amount) / NULLIF(COUNT(DISTINCT fo.order_id), 0), 2) AS avg_check
    FROM dds.fact_order fo
    JOIN dds.dim_date dd ON dd.id = fo.date_id
    JOIN dds.dim_user du ON du.id = fo.user_id
    GROUP BY dd.date
),
sessions AS (
    SELECT
        dd.date AS event_date,
        COUNT(*) AS total_sessions
    FROM dds.fact_session fs
    JOIN dds.dim_date dd ON dd.id = fs.date_id
    GROUP BY dd.date
),
sessions_with_orders AS (
    SELECT
        dd.date AS event_date,
        COUNT(DISTINCT fs.id) AS sessions_with_orders
    FROM dds.fact_session fs
    JOIN dds.dim_date dd ON dd.id = fs.date_id
    JOIN dds.fact_order fo ON fo.session_id = fs.session_dim_id
    GROUP BY dd.date
),
new_users AS (
    SELECT
        DATE(du.profile_created_at) AS event_date,
        COUNT(DISTINCT du.user_id) AS new_users
    FROM dds.dim_user du
    GROUP BY DATE(du.profile_created_at)
)
SELECT
    db.event_date,
    COALESCE(o.revenue, 0) AS revenue,
    COALESCE(o.orders_count, 0) AS orders_count,
    COALESCE(o.unique_users, 0) AS unique_users,
    COALESCE(o.avg_check, 0) AS avg_check,
    ROUND((COALESCE(swo.sessions_with_orders, 0)::numeric / NULLIF(s.total_sessions, 0)), 4) AS conversion_rate,
    COALESCE(nu.new_users, 0) AS new_users
FROM date_base db
LEFT JOIN orders o ON o.event_date = db.event_date
LEFT JOIN sessions s ON s.event_date = db.event_date
LEFT JOIN sessions_with_orders swo ON swo.event_date = db.event_date
LEFT JOIN new_users nu ON nu.event_date = db.event_date
ORDER BY db.event_date;