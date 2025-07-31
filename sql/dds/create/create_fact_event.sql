CREATE TABLE IF NOT EXISTS dds.fact_event (
    id SERIAL PRIMARY KEY,

    event_id UUID NOT NULL UNIQUE,
    event_type TEXT NOT NULL CHECK (event_type IN ('page_view', 'add_to_cart', 'purchase')),
    event_time TIMESTAMPTZ NOT NULL,
    event_date DATE NOT NULL,

    user_id UUID NOT NULL REFERENCES dds.dim_user(user_id) ON DELETE CASCADE,
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

    raw_payload JSONB NOT NULL,

    created_at TIMESTAMPTZ DEFAULT now()
);


CREATE INDEX IF NOT EXISTS idx_fact_event_event_time ON dds.fact_event(event_time);
CREATE INDEX IF NOT EXISTS idx_fact_event_user_id ON dds.fact_event(user_id);
CREATE INDEX IF NOT EXISTS idx_fact_event_session_id ON dds.fact_event(session_id);
CREATE INDEX IF NOT EXISTS idx_fact_event_event_type ON dds.fact_event(event_type);
CREATE INDEX IF NOT EXISTS idx_fact_event_marketing_id ON dds.fact_event(marketing_id);
