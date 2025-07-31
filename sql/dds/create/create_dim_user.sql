CREATE TABLE IF NOT EXISTS dds.dim_user (
    user_id UUID PRIMARY KEY,
    email TEXT NOT NULL,
    referral_code TEXT NOT NULL,
    user_name TEXT NOT NULL,
    birth_date DATE NOT NULL,
    profile_created_at TIMESTAMPTZ NOT NULL
);