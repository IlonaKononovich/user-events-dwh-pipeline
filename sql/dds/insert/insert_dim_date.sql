INSERT INTO dds.dim_date (date)
VALUES (DATE(%(event_time)s))
ON CONFLICT (date) DO NOTHING;
