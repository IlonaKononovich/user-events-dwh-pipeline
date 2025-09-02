CREATE TABLE IF NOT EXISTS dds.dim_product (
    id SERIAL PRIMARY KEY,
    product_id UUID NOT NULL UNIQUE,
    name TEXT NOT NULL,
    category TEXT NOT NULL,
    supplier TEXT NOT NULL,
    price NUMERIC(18, 2) NOT NULL CHECK (price >= 0)
);

COMMENT ON TABLE dds.dim_product IS 'Справочник продуктов. Содержит уникальные товары, категории и цены.';

COMMENT ON COLUMN dds.dim_product.id IS 'Суррогатный ключ продукта в DWH';
COMMENT ON COLUMN dds.dim_product.product_id IS 'Уникальный идентификатор продукта (UUID из источника)';
COMMENT ON COLUMN dds.dim_product.name IS 'Название продукта';
COMMENT ON COLUMN dds.dim_product.category IS 'Категория продукта';
COMMENT ON COLUMN dds.dim_product.supplier IS 'Поставщик продукта';
COMMENT ON COLUMN dds.dim_product.price IS 'Цена продукта (неотрицательная)';
