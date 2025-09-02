CREATE TABLE IF NOT EXISTS dds.dim_date (
    id SERIAL PRIMARY KEY,
    date DATE NOT NULL UNIQUE
);

COMMENT ON TABLE dds.dim_date IS 'Измерение: календарные даты для событий и заказов';
COMMENT ON COLUMN dds.dim_date.id IS 'Уникальный surrogate key даты';
COMMENT ON COLUMN dds.dim_date.date IS 'Календарная дата (YYYY-MM-DD)';