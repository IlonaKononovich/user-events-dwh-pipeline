INSERT INTO dds.dim_product (product_id, name, category, supplier, price)
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (product_id) DO UPDATE SET
    name = EXCLUDED.name,
    category = EXCLUDED.category,
    supplier = EXCLUDED.supplier,
    price = EXCLUDED.price
RETURNING id;