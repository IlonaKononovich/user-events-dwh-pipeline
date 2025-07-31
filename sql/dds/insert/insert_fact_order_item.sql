INSERT INTO dds.fact_order_item (
    order_id,
    product_id,
    quantity,
    price
)
SELECT 
    r.order_id,
    p.id AS product_id,
    (prod.product->>'quantity')::INT AS quantity,
    (prod.product->>'price')::NUMERIC(18,2) AS price
FROM raw.events r
JOIN LATERAL jsonb_array_elements(r.products) AS prod(product) ON TRUE
JOIN dds.dim_product p ON p.product_id = (prod.product->>'product_id')::UUID
JOIN dds.fact_order fo ON fo.order_id = r.order_id
LEFT JOIN dds.fact_order_item foi 
    ON foi.order_id = r.order_id AND foi.product_id = p.id
WHERE foi.id IS NULL
  AND r.order_id IS NOT NULL
  AND r.products IS NOT NULL;
