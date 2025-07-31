CREATE TABLE IF NOT EXISTS raw.events (
    id SERIAL PRIMARY KEY,
    event_id UUID NOT NULL,
    event_type TEXT NOT NULL,
    event_time TIMESTAMPTZ NOT NULL,
    event_date DATE NOT NULL,

    -- User
    user_id UUID NOT NULL,
    email TEXT NOT NULL,
    referral_code TEXT NOT NULL,
    user_name TEXT NOT NULL,
    birth_date DATE NOT NULL,
    profile_created_at TIMESTAMPTZ NOT NULL,

    -- Session
    session_id UUID NOT NULL,
    session_start_time TIMESTAMPTZ NOT NULL,
    session_end_time TIMESTAMPTZ NOT NULL,
    device_type TEXT NOT NULL CHECK (device_type IN ('mobile', 'desktop', 'tablet')),
    device_os TEXT NOT NULL CHECK (device_os IN ('Windows', 'iOS', 'Linux', 'Android')),
    location_country TEXT NOT NULL,
    location_city TEXT NOT NULL,
    pages_viewed INTEGER,

    -- Products — jsonb массив, так как может быть несколько товаров
    products JSONB,

    -- Order — nullable, т.к. может отсутствовать для не-покупок
    order_id UUID,
    payment_id UUID,
    order_items INTEGER,
    total_amount NUMERIC,
    order_status TEXT CHECK (order_status IN ('created', 'paid', 'shipped', 'cancelled') OR order_status IS NULL),

    -- Marketing — nullable
    campaign TEXT,
    promocode TEXT,
    user_campaign_id UUID,

    -- Полный исходный json события
    raw_payload JSONB NOT NULL,

    created_at TIMESTAMPTZ DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_events_event_id_unique ON raw.events(event_id);

