CREATE TABLE IF NOT EXISTS marts.user_behavior (
    user_id UUID COMMENT 'Уникальный идентификатор пользователя',
    sessions_count UInt64 COMMENT 'Общее количество сессий пользователя',
    avg_session_duration Float64 COMMENT 'Средняя длительность сессии пользователя (в секундах)',
    avg_pages_per_session Float64 COMMENT 'Среднее количество просмотренных страниц за сессию',
    events_count UInt64 COMMENT 'Общее количество событий пользователя',
    active_users_7d UInt64 COMMENT 'Количество активных пользователей за последние 7 дней',
    version UInt64 COMMENT 'Версия записи (для ReplacingMergeTree)'
) ENGINE = ReplacingMergeTree(version)
ORDER BY user_id
SETTINGS index_granularity = 8192
COMMENT 'Поведение пользователей: сессии, средняя длительность, просмотры страниц, активность, события';
