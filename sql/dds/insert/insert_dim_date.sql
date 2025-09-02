INSERT INTO dds.dim_date (date)
VALUES (%s)
ON CONFLICT (date) DO NOTHING
RETURNING id;
