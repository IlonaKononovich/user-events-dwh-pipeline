INSERT INTO dds.dim_session (
    session_id,
    user_id,
    start_time,
    end_time,
    device_id,
    location_id
)
SELECT DISTINCT
    r.session_id,
    u.user_id AS user_id,
    r.session_start_time,
    r.session_end_time,
    d.id AS device_id,
    l.id AS location_id
FROM raw.events r
JOIN dds.dim_user u ON u.user_id = r.user_id
JOIN dds.dim_device d ON d.device_type = r.device_type AND d.device_os = r.device_os
JOIN dds.dim_location l ON l.country = r.location_country AND l.city = r.location_city
LEFT JOIN dds.dim_session s ON s.session_id = r.session_id
WHERE s.session_id IS NULL;
