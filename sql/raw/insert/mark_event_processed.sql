INSERT INTO raw.processed_events (event_id)
VALUES (%s)
ON CONFLICT (event_id) DO NOTHING;
