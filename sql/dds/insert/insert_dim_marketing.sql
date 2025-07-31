INSERT INTO dds.dim_marketing (campaign, promocode, user_campaign_id)
SELECT DISTINCT
    campaign,
    promocode,
    user_campaign_id
FROM raw.events
WHERE campaign IS NOT NULL
   OR promocode IS NOT NULL
   OR user_campaign_id IS NOT NULL
ON CONFLICT ON CONSTRAINT ux_dim_marketing_unique DO NOTHING;
