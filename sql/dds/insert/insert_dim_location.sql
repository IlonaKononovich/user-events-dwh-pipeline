INSERT INTO dds.dim_location (country, city)
VALUES (%s, %s)
ON CONFLICT (country, city) DO NOTHING
RETURNING id;