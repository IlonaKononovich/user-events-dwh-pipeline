CREATE TABLE IF NOT EXISTS dds.dim_device (
    id SERIAL PRIMARY KEY,
    device_type TEXT NOT NULL CHECK (device_type IN ('mobile', 'desktop', 'tablet')),
    device_os TEXT NOT NULL CHECK (device_os IN ('Windows', 'iOS', 'Linux', 'Android')),
    UNIQUE(device_type, device_os)
);

COMMENT ON TABLE dds.dim_device IS 'Измерение: тип и ОС устройства';
COMMENT ON COLUMN dds.dim_device.id IS 'Уникальный surrogate key устройства';
COMMENT ON COLUMN dds.dim_device.device_type IS 'Тип устройства: mobile, desktop, tablet';
COMMENT ON COLUMN dds.dim_device.device_os IS 'Операционная система устройства: Windows, Linux, iOS, Android';