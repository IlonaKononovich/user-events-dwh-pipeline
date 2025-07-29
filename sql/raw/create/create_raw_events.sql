CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS raw.events (
    id SERIAL PRIMARY KEY,
    event_id UUID NOT NULL,
    event_time TIMESTAMPTZ NOT NULL,

    user_id UUID NOT NULL,
    email TEXT NOT NULL,
    referral_code TEXT NOT NULL,
    user_name TEXT NOT NULL,
    birth_date DATE NOT NULL,
    profile_created_at TIMESTAMPTZ NOT NULL,

    product_id UUID NOT NULL,
    product_name TEXT NOT NULL,
    category TEXT NOT NULL,
    supplier TEXT NOT NULL,
    price NUMERIC NOT NULL,

    session_id UUID NOT NULL,
    session_start_time TIMESTAMPTZ NOT NULL,
    session_end_time TIMESTAMPTZ NOT NULL,
    device_type TEXT NOT NULL,
    device_os TEXT NOT NULL,
    location_country TEXT NOT NULL,
    location_city TEXT NOT NULL,

    order_id UUID NOT NULL,
    payment_id UUID NOT NULL,
    order_items INTEGER NOT NULL,
    total_amount NUMERIC NOT NULL,
    order_status TEXT NOT NULL,

    campaign TEXT,
    promocode TEXT,
    user_campaign_id UUID,

    raw_payload JSONB NOT NULL,
    created_at TIMESTAMP DEFAULT now()
);
