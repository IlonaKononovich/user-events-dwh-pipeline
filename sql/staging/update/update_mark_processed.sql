UPDATE staging.events
SET processed_flg = TRUE
WHERE stg_id = %s;
