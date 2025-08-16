CREATE TABLE IF NOT EXISTS marts.daily_summary (
    event_date Date COMMENT 'Дата события',
    revenue Float64 COMMENT 'Общая выручка за день',
    orders_count UInt64 COMMENT 'Количество заказов за день',
    unique_users UInt64 COMMENT 'Количество уникальных пользователей за день',
    avg_check Float64 COMMENT 'Средний чек за день',
    conversion_rate Float64 COMMENT 'Конверсия (отношение покупок к сессиям)',
    new_users UInt64 COMMENT 'Количество новых пользователей за день',
    version UInt64 COMMENT 'Версия записи (для ReplacingMergeTree)'
) ENGINE = ReplacingMergeTree(version)
PARTITION BY toYYYYMM(event_date)
ORDER BY event_date
SETTINGS index_granularity = 8192
COMMENT 'Ежедневная сводка по ключевым метрикам: выручка, заказы, пользователи, конверсия, новые пользователи';
