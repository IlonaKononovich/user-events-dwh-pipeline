INSERT INTO dds.dim_device (device_type, device_os)
VALUES (%(device_type)s, %(device_os)s)
ON CONFLICT (device_type, device_os) DO NOTHING;
