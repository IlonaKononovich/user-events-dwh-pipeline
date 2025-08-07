"""
Порядки загрузки и создания таблиц для слоя DDS.

DIM_LOAD_ORDER — последовательность загрузки измерений (dimensions),
обеспечивающая корректную загрузку с учётом зависимостей.

FACT_LOAD_ORDER — последовательность загрузки фактов (fact tables).

CREATE_DDS_ORDER — порядок выполнения SQL-скриптов для создания таблиц DDS,
чтобы избежать ошибок из-за внешних ключей.
"""

# Колонки для вставки в raw.events
RAW_EVENT_COLUMNS = [
    "event_id", "event_type", "event_time", "event_date",
    "user_id", "email", "referral_code", "user_name", "birth_date", "profile_created_at",
    "session_id", "session_start_time", "session_end_time", "device_type", "device_os",
    "location_country", "location_city", "pages_viewed",
    "products",
    "order_id", "payment_id", "order_items", "total_amount", "order_status",
    "campaign", "promocode", "user_campaign_id",
    "raw_payload"
]

# Колонки для вставки в raw.events_invalid
RAW_EVENT_INVALID_COLUMNS = [
    "file_name", "event_id", "event_time", "error_message", "raw_payload"
]

DIM_LOAD_ORDER = [
    'dim_user',
    'dim_device',
    'dim_location',
    'dim_date',      
    'dim_product',
    'dim_session',
    'dim_marketing'
]


FACT_LOAD_ORDER = ['fact_session', 'fact_order', 'fact_order_item', 'fact_event']


CREATE_DDS_ORDER = [
    'create_dim_date.sql',
    'create_dim_user.sql',
    'create_dim_device.sql',
    'create_dim_location.sql',
    'create_dim_marketing.sql',
    'create_dim_product.sql',
    'create_dim_session.sql',
    'create_fact_event.sql',
    'create_fact_session.sql',
    'create_fact_order.sql',
    'create_fact_order_item.sql',
    ]
