    CREATE TABLE IF NOT EXISTS marts.user_behavior (
        user_id UUID,
        sessions_count UInt64,
        avg_session_duration Float64,
        pages_viewed UInt64,
        retention_rate Float64,
        version UInt64
    ) ENGINE = ReplacingMergeTree(version)
    ORDER BY user_id