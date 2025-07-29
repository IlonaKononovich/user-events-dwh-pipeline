CREATE TABLE IF NOT EXISTS dds.dim_device (
    id SERIAL PRIMARY KEY,
    device_type TEXT NOT NULL CHECK (device_type IN ('mobile', 'desktop', 'tablet')),
    device_os TEXT NOT NULL CHECK (device_os IN ('Windows', 'iOS', 'Linux', 'Android')),
    UNIQUE(device_type, device_os)
);
