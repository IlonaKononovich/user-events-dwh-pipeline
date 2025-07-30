    SELECT * FROM raw.events e
    WHERE NOT EXISTS (
        SELECT 1 FROM raw.processed_events p WHERE p.event_id = e.event_id
    )
    ORDER BY event_time;