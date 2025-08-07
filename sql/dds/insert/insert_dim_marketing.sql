INSERT INTO dds.dim_marketing (campaign, promocode, user_campaign_id)
VALUES (%s, %s, %s)
ON CONFLICT ON CONSTRAINT ux_dim_marketing_unique DO UPDATE SET
    campaign = EXCLUDED.campaign,
    promocode = EXCLUDED.promocode,
    user_campaign_id = EXCLUDED.user_campaign_id
RETURNING id;