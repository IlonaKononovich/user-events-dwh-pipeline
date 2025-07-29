CREATE TABLE IF NOT EXISTS dds.fact_order (
    id SERIAL PRIMARY KEY,
    order_id UUID NOT NULL UNIQUE,
    payment_id UUID NOT NULL,
    order_items INTEGER NOT NULL CHECK (order_items > 0),
    total_amount NUMERIC(18, 2) NOT NULL CHECK (total_amount >= 0),
    order_status TEXT NOT NULL CHECK (order_status IN ('created', 'paid', 'shipped', 'cancelled')),

    user_id INT NOT NULL REFERENCES dds.dim_user(id) ON DELETE CASCADE,
    product_id INT NOT NULL REFERENCES dds.dim_product(id) ON DELETE CASCADE,
    session_id INT NOT NULL REFERENCES dds.dim_session(id) ON DELETE CASCADE,
    device_id INT NOT NULL REFERENCES dds.dim_device(id) ON DELETE CASCADE,
    location_id INT NOT NULL REFERENCES dds.dim_location(id) ON DELETE CASCADE,
    date_id INT NOT NULL REFERENCES dds.dim_date(id) ON DELETE CASCADE,

    campaign TEXT,
    promocode TEXT,
    user_campaign_id UUID,

    event_time TIMESTAMP WITH TIME ZONE NOT NULL CHECK (event_time <= now()),
    raw_event_id UUID NOT NULL,

    UNIQUE(order_id),
    UNIQUE(payment_id)
);

-- Индексы для ускорения запросов по внешним ключам и дате
CREATE INDEX IF NOT EXISTS idx_fact_order_user_id ON dds.fact_order(user_id);
CREATE INDEX IF NOT EXISTS idx_fact_order_product_id ON dds.fact_order(product_id);
CREATE INDEX IF NOT EXISTS idx_fact_order_session_id ON dds.fact_order(session_id);
CREATE INDEX IF NOT EXISTS idx_fact_order_event_time ON dds.fact_order(event_time);
