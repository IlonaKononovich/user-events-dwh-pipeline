INSERT INTO dds.dim_date (date)
VALUES (%(event_date)s)
ON CONFLICT (date) DO NOTHING;
