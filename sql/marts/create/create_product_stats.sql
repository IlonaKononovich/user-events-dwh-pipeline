CREATE TABLE IF NOT EXISTS marts.product_stats (
    product_id UUID,
    product_name String,
    category_name String,
    total_sold UInt64,
    total_revenue Float64,
    avg_price Float64,
    orders_count UInt64,
    version UInt64
) ENGINE = ReplacingMergeTree(version)
ORDER BY product_id;
