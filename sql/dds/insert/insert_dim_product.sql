INSERT INTO dds.dim_product (product_id, name, category, supplier, price)
VALUES (%(product_id)s, %(product_name)s, %(category)s, %(supplier)s, %(price)s)
ON CONFLICT (product_id) DO UPDATE SET
    name = EXCLUDED.name,
    category = EXCLUDED.category,
    supplier = EXCLUDED.supplier,
    price = EXCLUDED.price;
