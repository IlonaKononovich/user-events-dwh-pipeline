SELECT *
FROM staging.events
WHERE processed_flg = FALSE
ORDER BY stg_loaded_at, stg_id
LIMIT 1000;