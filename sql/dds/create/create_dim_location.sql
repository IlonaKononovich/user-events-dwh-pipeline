CREATE TABLE IF NOT EXISTS dds.dim_location (
    id SERIAL PRIMARY KEY,
    country TEXT NOT NULL,
    city TEXT NOT NULL,
    UNIQUE(country, city)
);

COMMENT ON TABLE dds.dim_location IS 'Измерение: геолокация пользователя';
COMMENT ON COLUMN dds.dim_location.id IS 'Уникальный surrogate key локации';
COMMENT ON COLUMN dds.dim_location.country IS 'Страна пользователя';
COMMENT ON COLUMN dds.dim_location.city IS 'Город пользователя';