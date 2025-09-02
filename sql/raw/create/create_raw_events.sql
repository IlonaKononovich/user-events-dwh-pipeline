CREATE TABLE IF NOT EXISTS raw.events (
    id SERIAL PRIMARY KEY,
    event_id UUID NOT NULL,
    event_type TEXT NOT NULL,
    event_time TIMESTAMPTZ NOT NULL,
    event_date DATE NOT NULL,

    -- User
    user_id UUID NOT NULL,
    email TEXT NOT NULL,
    referral_code TEXT NOT NULL,
    user_name TEXT NOT NULL,
    birth_date DATE NOT NULL,
    profile_created_at TIMESTAMPTZ NOT NULL,

    -- Session
    session_id UUID NOT NULL,
    session_start_time TIMESTAMPTZ NOT NULL,
    session_end_time TIMESTAMPTZ NOT NULL,
    device_type TEXT NOT NULL CHECK (device_type IN ('mobile', 'desktop', 'tablet')),
    device_os TEXT NOT NULL CHECK (device_os IN ('Windows', 'iOS', 'Linux', 'Android')),
    location_country TEXT NOT NULL,
    location_city TEXT NOT NULL,
    pages_viewed INTEGER,

    -- Products — jsonb массив, так как может быть несколько товаров
    products JSONB,

    -- Order — nullable, т.к. может отсутствовать для не-покупок
    order_id UUID,
    payment_id UUID,
    order_items INTEGER,
    total_amount NUMERIC,
    order_status TEXT CHECK (order_status IN ('created', 'paid', 'shipped', 'cancelled') OR order_status IS NULL),

    -- Marketing — nullable
    campaign TEXT,
    promocode TEXT,
    user_campaign_id UUID,

    -- Полный исходный json события
    raw_payload JSONB NOT NULL,

    created_at TIMESTAMPTZ DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_events_event_id_unique ON raw.events(event_id);

-- Комментарий к таблице
COMMENT ON TABLE raw.events IS 'Сырой слой (RAW). Таблица хранит все сгенерированные события пользователей веб-приложения в неизменённом виде. Содержит данные о пользователях, сессиях, товарах, заказах и маркетинговых кампаниях.';

-- Комментарии к колонкам
COMMENT ON COLUMN raw.events.id IS 'Суррогатный ключ записи (автоинкремент).';
COMMENT ON COLUMN raw.events.event_id IS 'Уникальный идентификатор события (UUID).';
COMMENT ON COLUMN raw.events.event_type IS 'Тип события: page_view, add_to_cart, purchase.';
COMMENT ON COLUMN raw.events.event_time IS 'Время события (UTC, с точностью до секунды).';
COMMENT ON COLUMN raw.events.event_date IS 'Дата события (отдельное поле для удобства агрегации).';

COMMENT ON COLUMN raw.events.user_id IS 'UUID пользователя, к которому относится событие.';
COMMENT ON COLUMN raw.events.email IS 'Email пользователя.';
COMMENT ON COLUMN raw.events.referral_code IS 'Реферальный код, с которым пришёл пользователь.';
COMMENT ON COLUMN raw.events.user_name IS 'Имя пользователя (сгенерированное).';
COMMENT ON COLUMN raw.events.birth_date IS 'Дата рождения пользователя.';
COMMENT ON COLUMN raw.events.profile_created_at IS 'Дата и время создания профиля пользователя.';

COMMENT ON COLUMN raw.events.session_id IS 'UUID сессии пользователя.';
COMMENT ON COLUMN raw.events.session_start_time IS 'Время начала сессии.';
COMMENT ON COLUMN raw.events.session_end_time IS 'Время завершения сессии.';
COMMENT ON COLUMN raw.events.device_type IS 'Тип устройства: mobile, desktop, tablet.';
COMMENT ON COLUMN raw.events.device_os IS 'Операционная система устройства: Windows, iOS, Linux, Android.';
COMMENT ON COLUMN raw.events.location_country IS 'Страна, определённая по сессии (в генераторе всегда Беларусь).';
COMMENT ON COLUMN raw.events.location_city IS 'Город пользователя (выбирается случайным образом из списка).';
COMMENT ON COLUMN raw.events.pages_viewed IS 'Количество просмотренных страниц в рамках сессии.';

COMMENT ON COLUMN raw.events.products IS 'JSON-массив с товарами (product_id, name, category, supplier, price, quantity). Заполняется для add_to_cart и purchase.';
COMMENT ON COLUMN raw.events.order_id IS 'UUID заказа. Заполняется только для purchase.';
COMMENT ON COLUMN raw.events.payment_id IS 'UUID платежа. Заполняется только для purchase.';
COMMENT ON COLUMN raw.events.order_items IS 'Количество товаров в заказе. Заполняется только для purchase.';
COMMENT ON COLUMN raw.events.total_amount IS 'Сумма заказа. Заполняется только для purchase.';
COMMENT ON COLUMN raw.events.order_status IS 'Статус заказа: created, paid, shipped, cancelled (NULL для не-покупок).';

COMMENT ON COLUMN raw.events.campaign IS 'Название маркетинговой кампании (если событие связано с акцией).';
COMMENT ON COLUMN raw.events.promocode IS 'Использованный промокод (если применён).';
COMMENT ON COLUMN raw.events.user_campaign_id IS 'UUID кампании пользователя (идентификатор участия в акции).';

COMMENT ON COLUMN raw.events.raw_payload IS 'Полный исходный JSON события в формате jsonb.';
COMMENT ON COLUMN raw.events.created_at IS 'Время загрузки события в RAW-слой.';


