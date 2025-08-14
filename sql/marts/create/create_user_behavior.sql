CREATE TABLE IF NOT EXISTS marts.user_behavior (
    user_id UUID,
    sessions_count UInt64,
    avg_session_duration Float64,
    avg_pages_per_session Float64,
    events_count UInt64,
    active_users_7d UInt64,
    version UInt64
) ENGINE = ReplacingMergeTree(version)
ORDER BY user_id;
