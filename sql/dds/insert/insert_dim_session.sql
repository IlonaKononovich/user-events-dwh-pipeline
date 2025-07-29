WITH device AS (
    SELECT id FROM dds.dim_device WHERE device_type = %(device_type)s AND device_os = %(device_os)s
),
location AS (
    SELECT id FROM dds.dim_location WHERE country = %(location_country)s AND city = %(location_city)s
)
INSERT INTO dds.dim_session (session_id, start_time, end_time, device_id, location_id)
SELECT %(session_id)s, %(session_start_time)s, %(session_end_time)s, device.id, location.id
FROM device, location
ON CONFLICT (session_id) DO UPDATE SET
    start_time = EXCLUDED.start_time,
    end_time = EXCLUDED.end_time,
    device_id = EXCLUDED.device_id,
    location_id = EXCLUDED.location_id;
