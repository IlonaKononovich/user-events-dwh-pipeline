WITH products_agg AS (
    SELECT
        event_id,
        jsonb_agg(jsonb_build_object(
            'product_id', product_id,
            'product_name', product_name,
            'category_name', category_name,
            'supplier_name', supplier_name,
            'product_price', product_price,
            'product_quantity', product_quantity
        )) AS products
    FROM staging.events
    GROUP BY event_id
),
event_distinct AS (
    SELECT *
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY event_time DESC) AS rn
        FROM staging.events
    ) sub
    WHERE rn = 1
)

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
    order_status
)
SELECT
    e.event_id,
    e.event_type,
    e.event_time,
    e.event_date,
    u.id,
    s.id,
    d.id,
    l.id,
    dt.id,
    m.id,
    e.pages_viewed,
    p.products,
    e.order_id,
    e.payment_id,
    e.order_items,
    e.total_amount,
    e.order_status
FROM event_distinct e
JOIN products_agg p ON p.event_id = e.event_id
JOIN dds.dim_session s ON s.session_id = e.session_id
JOIN dds.dim_device d ON d.device_type = e.device_type AND d.device_os = e.device_os
JOIN dds.dim_location l ON l.country = e.location_country AND l.city = e.location_city
JOIN dds.dim_date dt ON dt.date = e.event_date
JOIN dds.dim_user u ON u.user_id = e.user_id
LEFT JOIN dds.dim_marketing m
    ON COALESCE(m.campaign, '') = COALESCE(e.campaign, '')
    AND COALESCE(m.promocode, '') = COALESCE(e.promocode, '')
    AND COALESCE(m.user_campaign_id::text, '') = COALESCE(e.user_campaign_id::text, '')
LEFT JOIN dds.fact_event fe ON fe.event_id = e.event_id
WHERE fe.event_id IS NULL
  AND e.event_type IN ('page_view', 'add_to_cart', 'purchase');
