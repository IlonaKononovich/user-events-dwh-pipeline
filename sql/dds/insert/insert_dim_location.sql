INSERT INTO dds.dim_location (country, city)
VALUES (%(location_country)s, %(location_city)s)
ON CONFLICT (country, city) DO NOTHING;
