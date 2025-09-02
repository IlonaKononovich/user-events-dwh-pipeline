CREATE TABLE IF NOT EXISTS dds.fact_session (
    id SERIAL PRIMARY KEY,

    session_dim_id INT NOT NULL UNIQUE REFERENCES dds.dim_session(id) ON DELETE CASCADE,
    date_id INT NOT NULL REFERENCES dds.dim_date(id) ON DELETE CASCADE,

    session_start_time TIMESTAMPTZ NOT NULL,
    session_end_time TIMESTAMPTZ NOT NULL,
    pages_viewed INT,
    events_count INT DEFAULT 0,
    is_converted BOOLEAN DEFAULT FALSE,

    created_at TIMESTAMPTZ DEFAULT now(),

    CONSTRAINT chk_end_after_start CHECK (session_end_time >= session_start_time)
);

CREATE INDEX IF NOT EXISTS idx_fact_session_date_id ON dds.fact_session(date_id);
CREATE INDEX IF NOT EXISTS idx_fact_session_start_time ON dds.fact_session(session_start_time);
CREATE INDEX IF NOT EXISTS idx_fact_session_is_converted ON dds.fact_session(is_converted);

COMMENT ON TABLE dds.fact_session IS 'Факт: агрегированная информация о сессиях';
COMMENT ON COLUMN dds.fact_session.id IS 'Уникальный surrogate key факта сессии';
COMMENT ON COLUMN dds.fact_session.session_dim_id IS 'FK на сессию (dds.dim_session.id)';
COMMENT ON COLUMN dds.fact_session.date_id IS 'FK на дату (dds.dim_date.id)';
COMMENT ON COLUMN dds.fact_session.session_start_time IS 'Время начала сессии';
COMMENT ON COLUMN dds.fact_session.session_end_time IS 'Время окончания сессии';
COMMENT ON COLUMN dds.fact_session.pages_viewed IS 'Количество просмотренных страниц за сессию';
COMMENT ON COLUMN dds.fact_session.events_count IS 'Количество событий в сессии';
COMMENT ON COLUMN dds.fact_session.is_converted IS 'Флаг: была ли конверсия в заказ';
COMMENT ON COLUMN dds.fact_session.created_at IS 'Время загрузки записи в DDS';