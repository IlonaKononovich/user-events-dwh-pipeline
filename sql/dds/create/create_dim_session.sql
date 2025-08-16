CREATE TABLE IF NOT EXISTS dds.dim_session (
    id SERIAL PRIMARY KEY,
    session_id UUID NOT NULL UNIQUE,
    user_id INT NOT NULL REFERENCES dds.dim_user(id) ON DELETE CASCADE,
    start_time TIMESTAMPTZ NOT NULL,
    end_time TIMESTAMPTZ NOT NULL,
    device_id INT NOT NULL REFERENCES dds.dim_device(id) ON DELETE CASCADE,
    location_id INT NOT NULL REFERENCES dds.dim_location(id) ON DELETE CASCADE,
    CONSTRAINT chk_end_after_start CHECK (end_time >= start_time)
);

COMMENT ON TABLE dds.dim_session IS 'Измерение: сессии пользователей с детализацией устройства и локации';
COMMENT ON COLUMN dds.dim_session.id IS 'Уникальный surrogate key сессии';
COMMENT ON COLUMN dds.dim_session.session_id IS 'UUID сессии';
COMMENT ON COLUMN dds.dim_session.user_id IS 'FK на пользователя (dds.dim_user.id)';
COMMENT ON COLUMN dds.dim_session.start_time IS 'Время начала сессии';
COMMENT ON COLUMN dds.dim_session.end_time IS 'Время окончания сессии';
COMMENT ON COLUMN dds.dim_session.device_id IS 'FK на устройство (dds.dim_device.id)';
COMMENT ON COLUMN dds.dim_session.location_id IS 'FK на локацию (dds.dim_location.id)';