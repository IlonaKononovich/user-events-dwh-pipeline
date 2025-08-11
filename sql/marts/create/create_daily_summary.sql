CREATE TABLE IF NOT EXISTS marts.daily_summary (
        event_date Date,
        revenue Float64,
        orders_count UInt64,
        unique_users UInt64,
        avg_check Float64,
        conversion_rate Float64,
        new_users UInt64,
        version UInt64
    ) ENGINE = ReplacingMergeTree(version)
    PARTITION BY toYYYYMM(event_date)
    ORDER BY event_date