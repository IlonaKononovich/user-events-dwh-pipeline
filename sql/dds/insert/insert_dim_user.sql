INSERT INTO dds.dim_user (user_id, email, referral_code, user_name, birth_date, profile_created_at)
VALUES (%(user_id)s, %(email)s, %(referral_code)s, %(user_name)s, %(birth_date)s, %(profile_created_at)s)
ON CONFLICT (user_id) DO UPDATE SET
    email = EXCLUDED.email,
    referral_code = EXCLUDED.referral_code,
    user_name = EXCLUDED.user_name,
    birth_date = EXCLUDED.birth_date,
    profile_created_at = EXCLUDED.profile_created_at;
