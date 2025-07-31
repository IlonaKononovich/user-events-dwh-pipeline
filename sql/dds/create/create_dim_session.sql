CREATE TABLE IF NOT EXISTS dds.dim_session (
    id SERIAL PRIMARY KEY,
    session_id UUID NOT NULL UNIQUE,
    user_id UUID NOT NULL REFERENCES dds.dim_user(user_id) ON DELETE CASCADE,
    start_time TIMESTAMP WITH TIME ZONE NOT NULL,
    end_time TIMESTAMP WITH TIME ZONE NOT NULL,
    device_id INT NOT NULL REFERENCES dds.dim_device(id) ON DELETE CASCADE,
    location_id INT NOT NULL REFERENCES dds.dim_location(id) ON DELETE CASCADE,

    CONSTRAINT chk_end_after_start CHECK (end_time >= start_time)
);
