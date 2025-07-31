CREATE TABLE IF NOT EXISTS dds.fact_order (
    id SERIAL PRIMARY KEY,
    
    order_id UUID NOT NULL UNIQUE,
    payment_id UUID NOT NULL UNIQUE,
    
    order_items INTEGER NOT NULL CHECK (order_items > 0),
    total_amount NUMERIC(18, 2) NOT NULL CHECK (total_amount >= 0),
    order_status TEXT NOT NULL CHECK (order_status IN ('created', 'paid', 'shipped', 'cancelled')),
    
    user_id UUID NOT NULL REFERENCES dds.dim_user(user_id) ON DELETE CASCADE,
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
