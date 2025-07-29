CREATE TABLE IF NOT EXISTS dds.dim_location (
    id SERIAL PRIMARY KEY,
    country TEXT NOT NULL,
    city TEXT NOT NULL,
    UNIQUE(country, city)
);
