CREATE TABLE IF NOT EXISTS dds.fact_session (
    id SERIAL PRIMARY KEY,

    session_dim_id INT NOT NULL UNIQUE REFERENCES dds.dim_session(id) ON DELETE CASCADE,
    date_id INT NOT NULL REFERENCES dds.dim_date(id) ON DELETE CASCADE,

    session_start_time TIMESTAMPTZ NOT NULL,
    session_end_time TIMESTAMPTZ NOT NULL,
    pages_viewed INT,
    events_count INT DEFAULT 0,
    is_converted BOOLEAN DEFAULT FALSE,

    created_at TIMESTAMPTZ DEFAULT now(),

    CONSTRAINT chk_end_after_start CHECK (session_end_time >= session_start_time)
);

CREATE INDEX IF NOT EXISTS idx_fact_session_date_id ON dds.fact_session(date_id);
CREATE INDEX IF NOT EXISTS idx_fact_session_start_time ON dds.fact_session(session_start_time);
CREATE INDEX IF NOT EXISTS idx_fact_session_is_converted ON dds.fact_session(is_converted);

