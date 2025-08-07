INSERT INTO dds.fact_order (
    order_id,
    payment_id,
    order_items,
    total_amount,
    order_status,
    user_id,
    session_id,
    device_id,
    location_id,
    date_id,
    marketing_id,
    event_time,
    raw_event_id
)
SELECT DISTINCT
    r.order_id,
    r.payment_id,
    r.order_items,
    COALESCE(r.total_amount, 0)::NUMERIC(18,2),
    r.order_status,
    u.id,
    s.id,
    d.id,
    l.id,
    dt.id,
    m.id,
    r.event_time,
    r.event_id
FROM staging.events r
JOIN dds.dim_user u ON u.user_id = r.user_id
JOIN dds.dim_session s ON s.session_id = r.session_id
JOIN dds.dim_device d ON d.device_type = r.device_type AND d.device_os = r.device_os
JOIN dds.dim_location l ON l.country = r.location_country AND l.city = r.location_city
JOIN dds.dim_date dt ON dt.date = r.event_date
LEFT JOIN dds.dim_marketing m
    ON COALESCE(m.campaign, '') = COALESCE(r.campaign, '')
    AND COALESCE(m.promocode, '') = COALESCE(r.promocode, '')
    AND COALESCE(m.user_campaign_id::text, '') = COALESCE(r.user_campaign_id::text, '')
LEFT JOIN dds.fact_order fo ON fo.order_id = r.order_id
WHERE fo.order_id IS NULL
  AND r.order_id IS NOT NULL
  AND r.payment_id IS NOT NULL
  AND r.order_items > 0
  AND r.order_status IN ('created', 'paid', 'shipped', 'cancelled');