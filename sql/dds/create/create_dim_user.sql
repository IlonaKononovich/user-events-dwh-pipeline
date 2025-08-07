CREATE TABLE IF NOT EXISTS dds.dim_user (
    id SERIAL PRIMARY KEY,
    user_id UUID UNIQUE NOT NULL,
    email TEXT NOT NULL,
    referral_code TEXT NOT NULL,
    user_name TEXT NOT NULL,
    birth_date DATE NOT NULL,
    profile_created_at TIMESTAMPTZ NOT NULL
);
