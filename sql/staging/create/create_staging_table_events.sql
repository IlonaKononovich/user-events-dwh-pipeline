CREATE TABLE IF NOT EXISTS staging.events (
    -- Тех. поля
    stg_id BIGSERIAL PRIMARY KEY,
    stg_loaded_at TIMESTAMPTZ DEFAULT now(),
    processed_flg BOOLEAN DEFAULT FALSE,

    -- Event
    event_id UUID NOT NULL,
    event_type TEXT NOT NULL,
    event_time TIMESTAMPTZ NOT NULL,
    event_date DATE NOT NULL,

    -- User (для dim_user)
    user_id UUID NOT NULL,
    user_email TEXT NOT NULL,
    user_name TEXT NOT NULL,
    referral_code TEXT,
    birth_date DATE,
    profile_created_at TIMESTAMPTZ,

    -- Device (для dim_device)
    session_id UUID NOT NULL,
    session_start TIMESTAMPTZ,
    session_end TIMESTAMPTZ,
    device_type TEXT,
    device_os TEXT,
    location_country TEXT,
    location_city TEXT,
    pages_viewed INT,

    -- Product (для dim_product)
    product_id UUID,
    product_name TEXT,
    category_name TEXT,
    supplier_name TEXT,
    product_price NUMERIC(18, 2),
    product_quantity INT,

    -- Order / Fact
    order_id UUID,
    payment_id UUID,
    order_items INT,
    total_amount NUMERIC(18, 2),
    order_status TEXT,

    -- Marketing
    campaign TEXT,
    promocode TEXT,
    user_campaign_id UUID
);

CREATE INDEX IF NOT EXISTS idx_stg_events_event_id ON staging.events(event_id);
CREATE INDEX IF NOT EXISTS idx_stg_events_user_id ON staging.events(user_id);
CREATE INDEX IF NOT EXISTS idx_stg_events_order_id ON staging.events(order_id);
