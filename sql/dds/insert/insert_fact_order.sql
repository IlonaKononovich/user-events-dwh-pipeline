WITH
user_dim AS (
    SELECT id FROM dds.dim_user WHERE user_id = %(user_id)s
),
product_dim AS (
    SELECT id FROM dds.dim_product WHERE product_id = %(product_id)s
),
session_dim AS (
    SELECT id FROM dds.dim_session WHERE session_id = %(session_id)s
),
device_dim AS (
    SELECT id FROM dds.dim_device WHERE device_type = %(device_type)s AND device_os = %(device_os)s
),
location_dim AS (
    SELECT id FROM dds.dim_location WHERE country = %(location_country)s AND city = %(location_city)s
),
date_dim AS (
    SELECT id FROM dds.dim_date WHERE date = DATE(%(event_time)s)
)
INSERT INTO dds.fact_order (
    order_id, payment_id, order_items, total_amount, order_status,
    user_id, product_id, session_id, device_id, location_id, date_id,
    campaign, promocode, user_campaign_id,
    event_time, raw_event_id
)
SELECT
    %(order_id)s, %(payment_id)s, %(order_items)s, %(total_amount)s, %(order_status)s,
    user_dim.id, product_dim.id, session_dim.id, device_dim.id, location_dim.id, date_dim.id,
    %(campaign)s, %(promocode)s, %(user_campaign_id)s,
    %(event_time)s, %(event_id)s
FROM user_dim, product_dim, session_dim, device_dim, location_dim, date_dim
ON CONFLICT (order_id) DO NOTHING;
