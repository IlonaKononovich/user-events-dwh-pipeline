INSERT INTO dds.fact_order_item (
    order_id,
    product_id,
    quantity,
    price
)
SELECT DISTINCT
    fo.order_id,
    p.id AS product_id,
    r.product_quantity AS quantity,
    r.product_price AS price
FROM staging.events r
JOIN dds.dim_product p ON p.product_id = r.product_id
JOIN dds.fact_order fo ON fo.order_id = r.order_id
LEFT JOIN dds.fact_order_item foi 
    ON foi.order_id = fo.order_id AND foi.product_id = p.id
WHERE foi.id IS NULL
  AND r.order_id IS NOT NULL
  AND r.product_id IS NOT NULL;
