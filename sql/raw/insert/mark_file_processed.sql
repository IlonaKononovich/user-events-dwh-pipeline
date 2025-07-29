INSERT INTO raw.processed_files (filename)
VALUES (%s)
ON CONFLICT (filename) DO NOTHING;