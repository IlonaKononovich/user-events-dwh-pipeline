INSERT INTO dds.dim_device (device_type, device_os)
VALUES (%s, %s)
ON CONFLICT (device_type, device_os) DO NOTHING
RETURNING id;