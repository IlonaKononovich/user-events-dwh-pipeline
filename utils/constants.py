"""
Порядки загрузки и создания таблиц для слоя DDS.

DIM_LOAD_ORDER — последовательность загрузки измерений (dimensions),
обеспечивающая корректную загрузку с учётом зависимостей.

FACT_LOAD_ORDER — последовательность загрузки фактов (fact tables).

CREATE_DDS_ORDER — порядок выполнения SQL-скриптов для создания таблиц DDS,
чтобы избежать ошибок из-за внешних ключей.
"""


DIM_LOAD_ORDER = [
    'dim_date',
    'dim_user',
    'dim_device',
    'dim_location',
    'dim_marketing',
    'dim_product',
    'dim_session',
]

FACT_LOAD_ORDER = [
    'fact_event',
    'fact_session',
    'fact_order',
    'fact_order_item',
]

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
