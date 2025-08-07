INSERT INTO staging.events (
    event_id,
    event_type,
    event_time,
    event_date,

    user_id,
    user_email,
    user_name,
    referral_code,
    birth_date,
    profile_created_at,

    session_id,
    session_start,
    session_end,
    device_type,
    device_os,
    location_country,
    location_city,
    pages_viewed,

    product_id,
    product_name,
    category_name,
    supplier_name,
    product_price,
    product_quantity,

    order_id,
    payment_id,
    order_items,
    total_amount,
    order_status,

    campaign,
    promocode,
    user_campaign_id
)
SELECT
    e.event_id,
    e.event_type,
    e.event_time,
    e.event_date,

    e.user_id,
    e.email,
    e.user_name,
    e.referral_code,
    e.birth_date,
    e.profile_created_at,

    e.session_id,
    e.session_start_time,
    e.session_end_time,
    e.device_type,
    e.device_os,
    e.location_country,
    e.location_city,
    e.pages_viewed,

    p.product_id,
    p.name AS product_name,
    p.category AS category_name,
    p.supplier AS supplier_name,
    p.price AS product_price,
    p.quantity AS product_quantity,

    e.order_id,
    e.payment_id,
    e.order_items,
    e.total_amount,
    e.order_status,

    e.campaign,
    e.promocode,
    e.user_campaign_id
FROM raw.events e
LEFT JOIN LATERAL
    jsonb_to_recordset(e.products) AS p(
        name TEXT,
        price NUMERIC(18,2),
        category TEXT,
        supplier TEXT,
        product_id UUID,
        quantity INT
    )
ON TRUE
WHERE NOT EXISTS (
    SELECT 1
    FROM staging.events s
    WHERE s.event_id = e.event_id
      AND (s.product_id = p.product_id OR (s.product_id IS NULL AND p.product_id IS NULL))
)
ORDER BY e.event_time
LIMIT {batch_limit};
