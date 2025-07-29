CREATE TABLE IF NOT EXISTS dds.dim_session (
    id SERIAL PRIMARY KEY,
    session_id UUID NOT NULL UNIQUE,
    start_time TIMESTAMP WITH TIME ZONE NOT NULL,
    end_time TIMESTAMP WITH TIME ZONE NOT NULL,
    device_id INT NOT NULL REFERENCES dds.dim_device(id),
    location_id INT NOT NULL REFERENCES dds.dim_location(id)
);
