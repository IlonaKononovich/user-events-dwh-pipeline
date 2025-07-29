CREATE TABLE IF NOT EXISTS raw.processed_events (
    event_id UUID PRIMARY KEY,
    processed_at TIMESTAMPTZ DEFAULT now()
);
