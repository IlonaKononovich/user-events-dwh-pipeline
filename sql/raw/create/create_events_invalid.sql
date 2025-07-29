CREATE TABLE IF NOT EXISTS raw.events_invalid (
    id SERIAL PRIMARY KEY,
    raw_payload JSONB,
    error TEXT,
    created_at TIMESTAMPTZ DEFAULT now()
);
