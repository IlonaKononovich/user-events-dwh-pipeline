CREATE TABLE IF NOT EXISTS raw.processed_files (
    filename TEXT PRIMARY KEY,
    processed_at TIMESTAMPTZ DEFAULT now()
);