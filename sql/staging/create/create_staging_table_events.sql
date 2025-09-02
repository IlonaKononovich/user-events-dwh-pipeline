CREATE TABLE IF NOT EXISTS staging.events (
    -- Тех. поля
    stg_id BIGSERIAL PRIMARY KEY,
    stg_loaded_at TIMESTAMPTZ DEFAULT now(),
    processed_flg BOOLEAN DEFAULT FALSE,

    -- Event
    event_id UUID NOT NULL,
    event_type TEXT NOT NULL,
    event_time TIMESTAMPTZ NOT NULL,
    event_date DATE NOT NULL,

    -- User (для dim_user)
    user_id UUID NOT NULL,
    user_email TEXT NOT NULL,
    user_name TEXT NOT NULL,
    referral_code TEXT,
    birth_date DATE,
    profile_created_at TIMESTAMPTZ,

    -- Device (для dim_device)
    session_id UUID NOT NULL,
    session_start TIMESTAMPTZ,
    session_end TIMESTAMPTZ,
    device_type TEXT,
    device_os TEXT,
    location_country TEXT,
    location_city TEXT,
    pages_viewed INT,

    -- Product (для dim_product)
    product_id UUID,
    product_name TEXT,
    category_name TEXT,
    supplier_name TEXT,
    product_price NUMERIC(18, 2),
    product_quantity INT,

    -- Order / Fact
    order_id UUID,
    payment_id UUID,
    order_items INT,
    total_amount NUMERIC(18, 2),
    order_status TEXT,

    -- Marketing
    campaign TEXT,
    promocode TEXT,
    user_campaign_id UUID
);

CREATE INDEX IF NOT EXISTS idx_stg_events_event_id ON staging.events(event_id);
CREATE INDEX IF NOT EXISTS idx_stg_events_user_id ON staging.events(user_id);
CREATE INDEX IF NOT EXISTS idx_stg_events_order_id ON staging.events(order_id);

COMMENT ON TABLE staging.events IS 'Промежуточный слой (staging) для хранения событий после загрузки из raw, но до валидации и загрузки в DDS';

COMMENT ON COLUMN staging.events.stg_id IS 'Технический surrogate key, уникальный идентификатор записи в staging';
COMMENT ON COLUMN staging.events.stg_loaded_at IS 'Время загрузки записи в staging';
COMMENT ON COLUMN staging.events.processed_flg IS 'Флаг обработки записи (false — не обработана, true — загружена в DDS)';
COMMENT ON COLUMN staging.events.event_id IS 'Уникальный идентификатор события (UUID)';
COMMENT ON COLUMN staging.events.event_type IS 'Тип события (например: просмотр, заказ, оплата)';
COMMENT ON COLUMN staging.events.event_time IS 'Время события (точное, с таймзоной)';
COMMENT ON COLUMN staging.events.event_date IS 'Дата события (без времени)';
COMMENT ON COLUMN staging.events.user_id IS 'Уникальный идентификатор пользователя (UUID)';
COMMENT ON COLUMN staging.events.user_email IS 'Email пользователя';
COMMENT ON COLUMN staging.events.user_name IS 'Имя пользователя';
COMMENT ON COLUMN staging.events.referral_code IS 'Реферальный код пользователя (если был)';
COMMENT ON COLUMN staging.events.birth_date IS 'Дата рождения пользователя';
COMMENT ON COLUMN staging.events.profile_created_at IS 'Дата создания профиля пользователя';
COMMENT ON COLUMN staging.events.session_id IS 'Идентификатор сессии (UUID)';
COMMENT ON COLUMN staging.events.session_start IS 'Время начала сессии';
COMMENT ON COLUMN staging.events.session_end IS 'Время окончания сессии';
COMMENT ON COLUMN staging.events.device_type IS 'Тип устройства (mobile, desktop, tablet)';
COMMENT ON COLUMN staging.events.device_os IS 'Операционная система устройства (Windows, Linux, iOS, Android)';
COMMENT ON COLUMN staging.events.location_country IS 'Страна пользователя (по геолокации)';
COMMENT ON COLUMN staging.events.location_city IS 'Город пользователя (по геолокации)';
COMMENT ON COLUMN staging.events.pages_viewed IS 'Количество просмотренных страниц в рамках события/сессии';
COMMENT ON COLUMN staging.events.product_id IS 'UUID товара (если событие связано с товаром)';
COMMENT ON COLUMN staging.events.product_name IS 'Название товара';
COMMENT ON COLUMN staging.events.category_name IS 'Категория товара';
COMMENT ON COLUMN staging.events.supplier_name IS 'Поставщик товара';
COMMENT ON COLUMN staging.events.product_price IS 'Цена товара';
COMMENT ON COLUMN staging.events.product_quantity IS 'Количество единиц товара';
COMMENT ON COLUMN staging.events.order_id IS 'UUID заказа (если событие связано с заказом)';
COMMENT ON COLUMN staging.events.payment_id IS 'UUID платежа (если есть)';
COMMENT ON COLUMN staging.events.order_items IS 'Количество позиций в заказе';
COMMENT ON COLUMN staging.events.total_amount IS 'Сумма заказа';
COMMENT ON COLUMN staging.events.order_status IS 'Статус заказа (created, paid, shipped, cancelled)';
COMMENT ON COLUMN staging.events.campaign IS 'Маркетинговая кампания';
COMMENT ON COLUMN staging.events.promocode IS 'Использованный промокод';
COMMENT ON COLUMN staging.events.user_campaign_id IS 'UUID связи пользователя с кампанией';
