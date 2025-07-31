CREATE TABLE IF NOT EXISTS dds.dim_marketing (
    id SERIAL PRIMARY KEY,
    campaign TEXT,
    promocode TEXT,
    user_campaign_id UUID,
    CONSTRAINT ux_dim_marketing_unique UNIQUE (campaign, promocode, user_campaign_id)
);
