SELECT
  dp.product_id,
  dp.name AS product_name,
  dp.category AS category_name,
  SUM(foi.quantity) AS total_sold,
  SUM(foi.quantity * foi.price) AS total_revenue,
  AVG(foi.price) AS avg_price,
  EXTRACT(EPOCH FROM now())::bigint AS version
FROM dds.fact_order_item foi
JOIN dds.dim_product dp ON dp.id = foi.product_id
JOIN dds.fact_order fo ON fo.order_id = foi.order_id
GROUP BY dp.product_id, dp.name, dp.category
ORDER BY dp.product_id
