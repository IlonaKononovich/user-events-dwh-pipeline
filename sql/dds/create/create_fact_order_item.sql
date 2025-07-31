CREATE TABLE IF NOT EXISTS dds.fact_order_item (
    id SERIAL PRIMARY KEY,
    order_id UUID NOT NULL REFERENCES dds.fact_order(order_id) ON DELETE CASCADE,
    product_id INT NOT NULL REFERENCES dds.dim_product(id) ON DELETE CASCADE,
    quantity INTEGER NOT NULL CHECK (quantity > 0),
    price NUMERIC(18, 2) NOT NULL CHECK (price >= 0),

    UNIQUE(order_id, product_id)
);
