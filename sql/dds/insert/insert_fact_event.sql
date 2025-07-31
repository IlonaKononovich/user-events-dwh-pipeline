INSERT INTO dds.fact_event (
    event_id,
    event_type,
    event_time,
    event_date,
    user_id,
    session_id,
    device_id,
    location_id,
    date_id,
    marketing_id,
    pages_viewed,
    products,
    order_id,
    payment_id,
    order_items,
    total_amount,
    order_status,
    raw_payload
)
SELECT
    r.event_id,
    r.event_type,
    r.event_time,
    r.event_date,
    u.user_id AS user_id,
    s.id AS session_id,
    d.id AS device_id,
    l.id AS location_id,
    dt.id AS date_id,
    m.id AS marketing_id,
    r.pages_viewed,
    r.products,
    r.order_id,
    r.payment_id,
    r.order_items,
    r.total_amount,
    r.order_status,
    r.raw_payload
FROM raw.events r
JOIN dds.dim_user u ON u.user_id = r.user_id
JOIN dds.dim_session s ON s.session_id = r.session_id
JOIN dds.dim_device d ON d.device_type = r.device_type AND d.device_os = r.device_os
JOIN dds.dim_location l ON l.country = r.location_country AND l.city = r.location_city
JOIN dds.dim_date dt ON dt.date = r.event_date
LEFT JOIN dds.dim_marketing m
    ON COALESCE(m.campaign, '') = COALESCE(r.campaign, '')
    AND COALESCE(m.promocode, '') = COALESCE(r.promocode, '')
    AND COALESCE(m.user_campaign_id::text, '') = COALESCE(r.user_campaign_id::text, '')
LEFT JOIN dds.fact_event fe ON fe.event_id = r.event_id
WHERE fe.event_id IS NULL
  AND r.event_type IN ('page_view', 'add_to_cart', 'purchase');
