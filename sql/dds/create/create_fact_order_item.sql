CREATE TABLE IF NOT EXISTS dds.fact_order_item (
    id SERIAL PRIMARY KEY,
    order_id UUID NOT NULL REFERENCES dds.fact_order(order_id) ON DELETE CASCADE,
    product_id INT NOT NULL REFERENCES dds.dim_product(id) ON DELETE CASCADE,
    quantity INTEGER NOT NULL CHECK (quantity > 0),
    price NUMERIC(18, 2) NOT NULL CHECK (price >= 0),

    UNIQUE(order_id, product_id)
);

COMMENT ON TABLE dds.fact_order_item IS 'Факт: состав заказа (товары и их количество)';
COMMENT ON COLUMN dds.fact_order_item.id IS 'Уникальный surrogate key позиции заказа';
COMMENT ON COLUMN dds.fact_order_item.order_id IS 'FK на заказ (dds.fact_order.order_id)';
COMMENT ON COLUMN dds.fact_order_item.product_id IS 'FK на продукт (dds.dim_product.id)';
COMMENT ON COLUMN dds.fact_order_item.quantity IS 'Количество единиц товара в заказе';
COMMENT ON COLUMN dds.fact_order_item.price IS 'Цена за единицу товара';