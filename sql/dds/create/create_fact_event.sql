CREATE TABLE IF NOT EXISTS dds.fact_event (
    id SERIAL PRIMARY KEY,

    event_id UUID NOT NULL UNIQUE,
    event_type TEXT NOT NULL CHECK (event_type IN ('page_view', 'add_to_cart', 'purchase')),
    event_time TIMESTAMPTZ NOT NULL,
    event_date DATE NOT NULL,

    user_id INT NOT NULL REFERENCES dds.dim_user(id) ON DELETE CASCADE,
    session_id INT NOT NULL REFERENCES dds.dim_session(id) ON DELETE CASCADE,
    device_id INT NOT NULL REFERENCES dds.dim_device(id) ON DELETE CASCADE,
    location_id INT NOT NULL REFERENCES dds.dim_location(id) ON DELETE CASCADE,
    date_id INT NOT NULL REFERENCES dds.dim_date(id) ON DELETE CASCADE,
    marketing_id INT REFERENCES dds.dim_marketing(id) ON DELETE SET NULL,

    pages_viewed INTEGER,
    products JSONB,

    order_id UUID,
    payment_id UUID,
    order_items INTEGER,
    total_amount NUMERIC(18, 2),
    order_status TEXT CHECK (order_status IN ('created', 'paid', 'shipped', 'cancelled')),


    created_at TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_fact_event_event_time ON dds.fact_event(event_time);
CREATE INDEX IF NOT EXISTS idx_fact_event_user_id ON dds.fact_event(user_id);
CREATE INDEX IF NOT EXISTS idx_fact_event_session_id ON dds.fact_event(session_id);
CREATE INDEX IF NOT EXISTS idx_fact_event_event_type ON dds.fact_event(event_type);
CREATE INDEX IF NOT EXISTS idx_fact_event_marketing_id ON dds.fact_event(marketing_id);

COMMENT ON TABLE dds.fact_event IS 'Факт: события пользователей (просмотр страниц, корзина, заказ)';
COMMENT ON COLUMN dds.fact_event.id IS 'Уникальный surrogate key события';
COMMENT ON COLUMN dds.fact_event.event_id IS 'UUID события (бизнес-ключ)';
COMMENT ON COLUMN dds.fact_event.event_type IS 'Тип события: page_view, add_to_cart, purchase';
COMMENT ON COLUMN dds.fact_event.event_time IS 'Время события';
COMMENT ON COLUMN dds.fact_event.event_date IS 'Дата события';
COMMENT ON COLUMN dds.fact_event.user_id IS 'FK на пользователя (dds.dim_user.id)';
COMMENT ON COLUMN dds.fact_event.session_id IS 'FK на сессию (dds.dim_session.id)';
COMMENT ON COLUMN dds.fact_event.device_id IS 'FK на устройство (dds.dim_device.id)';
COMMENT ON COLUMN dds.fact_event.location_id IS 'FK на локацию (dds.dim_location.id)';
COMMENT ON COLUMN dds.fact_event.date_id IS 'FK на дату (dds.dim_date.id)';
COMMENT ON COLUMN dds.fact_event.marketing_id IS 'FK на маркетинговую кампанию (dds.dim_marketing.id)';
COMMENT ON COLUMN dds.fact_event.pages_viewed IS 'Количество просмотренных страниц';
COMMENT ON COLUMN dds.fact_event.products IS 'JSONB: товары, участвующие в событии';
COMMENT ON COLUMN dds.fact_event.order_id IS 'UUID заказа, если событие связано с заказом';
COMMENT ON COLUMN dds.fact_event.payment_id IS 'UUID платежа, если применимо';
COMMENT ON COLUMN dds.fact_event.order_items IS 'Количество товаров в заказе';
COMMENT ON COLUMN dds.fact_event.total_amount IS 'Общая сумма заказа';
COMMENT ON COLUMN dds.fact_event.order_status IS 'Статус заказа: created, paid, shipped, cancelled';
COMMENT ON COLUMN dds.fact_event.created_at IS 'Время загрузки записи в DDS';