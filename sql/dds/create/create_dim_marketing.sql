CREATE TABLE IF NOT EXISTS dds.dim_marketing (
    id SERIAL PRIMARY KEY,
    campaign TEXT,
    promocode TEXT,
    user_campaign_id UUID,
    CONSTRAINT ux_dim_marketing_unique UNIQUE (campaign, promocode, user_campaign_id)
);

COMMENT ON TABLE dds.dim_marketing IS 'Измерение: маркетинговые кампании и промокоды';
COMMENT ON COLUMN dds.dim_marketing.id IS 'Уникальный surrogate key маркетинговой кампании';
COMMENT ON COLUMN dds.dim_marketing.campaign IS 'Название маркетинговой кампании';
COMMENT ON COLUMN dds.dim_marketing.promocode IS 'Промокод, связанный с кампанией';
COMMENT ON COLUMN dds.dim_marketing.user_campaign_id IS 'UUID связи пользователя с кампанией';