CREATE TABLE IF NOT EXISTS dds.dim_product (
    id SERIAL PRIMARY KEY,
    product_id UUID NOT NULL UNIQUE,
    name TEXT NOT NULL,
    category TEXT NOT NULL,
    supplier TEXT NOT NULL,
    price NUMERIC(18, 2) NOT NULL CHECK (price >= 0)
);
