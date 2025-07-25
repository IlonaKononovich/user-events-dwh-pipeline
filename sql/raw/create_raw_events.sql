CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS raw.events (
    id SERIAL PRIMARY KEY,
    event_id UUID NOT NULL,
    event_time TIMESTAMP WITH TIME ZONE NOT NULL,

    user_id UUID NOT NULL,
    email TEXT,
    referral_code TEXT,
    user_name TEXT,
    birth_date DATE,
    profile_created_at TIMESTAMP WITH TIME ZONE,

    product_id UUID,
    product_name TEXT,
    category TEXT,
    supplier TEXT,
    price NUMERIC,

    session_id UUID,
    session_start_time TIMESTAMP WITH TIME ZONE,
    session_end_time TIMESTAMP WITH TIME ZONE,
    device_type TEXT,
    device_os TEXT,
    location_country TEXT,
    location_city TEXT,

    order_id UUID,
    payment_id UUID,
    order_items INTEGER,
    total_amount NUMERIC,
    order_status TEXT,

    campaign TEXT,
    promocode TEXT,
    user_campaign_id UUID,

    raw_payload JSONB,
    created_at TIMESTAMP DEFAULT NOW()
);
