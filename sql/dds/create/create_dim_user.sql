CREATE TABLE IF NOT EXISTS dds.dim_user (
    id SERIAL PRIMARY KEY,
    user_id UUID NOT NULL UNIQUE,
    email TEXT NOT NULL,
    referral_code TEXT,
    user_name TEXT NOT NULL,
    birth_date DATE NOT NULL,
    profile_created_at TIMESTAMP WITH TIME ZONE NOT NULL
);
