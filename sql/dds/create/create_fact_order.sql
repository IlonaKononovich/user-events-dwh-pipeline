CREATE TABLE IF NOT EXISTS dds.fact_order (
    id SERIAL PRIMARY KEY,
    
    order_id UUID NOT NULL UNIQUE,
    payment_id UUID NOT NULL UNIQUE,
    
    order_items INTEGER NOT NULL CHECK (order_items > 0),
    total_amount NUMERIC(18, 2) NOT NULL CHECK (total_amount >= 0),
    order_status TEXT NOT NULL CHECK (order_status IN ('created', 'paid', 'shipped', 'cancelled')),
    
    user_id INT NOT NULL REFERENCES dds.dim_user(id) ON DELETE CASCADE,
    session_id INT NOT NULL REFERENCES dds.dim_session(id) ON DELETE CASCADE,
    device_id INT NOT NULL REFERENCES dds.dim_device(id) ON DELETE CASCADE,
    location_id INT NOT NULL REFERENCES dds.dim_location(id) ON DELETE CASCADE,
    date_id INT NOT NULL REFERENCES dds.dim_date(id) ON DELETE CASCADE,
    marketing_id INT REFERENCES dds.dim_marketing(id) ON DELETE SET NULL,
    
    event_time TIMESTAMPTZ NOT NULL CHECK (event_time <= now()),
    raw_event_id UUID NOT NULL REFERENCES raw.events(event_id) ON DELETE NO ACTION,

    created_at TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_fact_order_user_id ON dds.fact_order(user_id);
CREATE INDEX IF NOT EXISTS idx_fact_order_session_id ON dds.fact_order(session_id);
CREATE INDEX IF NOT EXISTS idx_fact_order_event_time ON dds.fact_order(event_time);
CREATE INDEX IF NOT EXISTS idx_fact_order_marketing_id ON dds.fact_order(marketing_id);

COMMENT ON TABLE dds.fact_order IS 'Факт: заказы пользователей';
COMMENT ON COLUMN dds.fact_order.id IS 'Уникальный surrogate key заказа';
COMMENT ON COLUMN dds.fact_order.order_id IS 'UUID заказа (бизнес-ключ)';
COMMENT ON COLUMN dds.fact_order.payment_id IS 'UUID платежа (бизнес-ключ)';
COMMENT ON COLUMN dds.fact_order.order_items IS 'Количество позиций в заказе';
COMMENT ON COLUMN dds.fact_order.total_amount IS 'Общая сумма заказа';
COMMENT ON COLUMN dds.fact_order.order_status IS 'Статус заказа: created, paid, shipped, cancelled';
COMMENT ON COLUMN dds.fact_order.user_id IS 'FK на пользователя (dds.dim_user.id)';
COMMENT ON COLUMN dds.fact_order.session_id IS 'FK на сессию (dds.dim_session.id)';
COMMENT ON COLUMN dds.fact_order.device_id IS 'FK на устройство (dds.dim_device.id)';
COMMENT ON COLUMN dds.fact_order.location_id IS 'FK на локацию (dds.dim_location.id)';
COMMENT ON COLUMN dds.fact_order.date_id IS 'FK на дату (dds.dim_date.id)';
COMMENT ON COLUMN dds.fact_order.marketing_id IS 'FK на маркетинговую кампанию (dds.dim_marketing.id)';
COMMENT ON COLUMN dds.fact_order.event_time IS 'Время события, связанного с заказом';
COMMENT ON COLUMN dds.fact_order.raw_event_id IS 'FK на raw.events.event_id';
COMMENT ON COLUMN dds.fact_order.created_at IS 'Время загрузки записи в DDS';
