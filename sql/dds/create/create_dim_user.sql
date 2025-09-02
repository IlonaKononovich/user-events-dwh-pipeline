CREATE TABLE IF NOT EXISTS dds.dim_user (
    id SERIAL PRIMARY KEY,
    user_id UUID UNIQUE NOT NULL,
    email TEXT NOT NULL,
    referral_code TEXT NOT NULL,
    user_name TEXT NOT NULL,
    birth_date DATE NOT NULL,
    profile_created_at TIMESTAMPTZ NOT NULL
);

COMMENT ON TABLE dds.dim_user IS 'Измерение: пользователи';
COMMENT ON COLUMN dds.dim_user.id IS 'Уникальный surrogate key пользователя';
COMMENT ON COLUMN dds.dim_user.user_id IS 'UUID пользователя (бизнес-ключ)';
COMMENT ON COLUMN dds.dim_user.email IS 'Email пользователя';
COMMENT ON COLUMN dds.dim_user.referral_code IS 'Реферальный код пользователя';
COMMENT ON COLUMN dds.dim_user.user_name IS 'Имя пользователя';
COMMENT ON COLUMN dds.dim_user.birth_date IS 'Дата рождения пользователя';
COMMENT ON COLUMN dds.dim_user.profile_created_at IS 'Дата создания профиля пользователя';