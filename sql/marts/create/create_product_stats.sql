CREATE TABLE IF NOT EXISTS marts.product_stats (
    product_id UUID COMMENT 'Уникальный идентификатор товара',
    product_name String COMMENT 'Название товара',
    category_name String COMMENT 'Название категории товара',
    total_sold UInt64 COMMENT 'Общее количество проданных единиц товара',
    total_revenue Float64 COMMENT 'Общая выручка с продаж товара',
    avg_price Float64 COMMENT 'Средняя цена продажи товара',
    orders_count UInt64 COMMENT 'Количество заказов, включающих этот товар',
    version UInt64 COMMENT 'Версия записи (для ReplacingMergeTree)'
) ENGINE = ReplacingMergeTree(version)
ORDER BY product_id
SETTINGS index_granularity = 8192
COMMENT 'Статистика по товарам: продажи, выручка, средняя цена, количество заказов';
