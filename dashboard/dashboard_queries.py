"""
dashboard_queries.py

Файл содержит SQL-запросы для построения дашбордов учебного проекта.

Мини-инструкция:
- Каждый блок содержит запросы для определенной метрики или отчета.
- KPI: суммарные показатели (выручка, заказы, конверсия, средний чек).
- Динамика по дням: показывает изменение метрик во времени.
- Продукты и категории: показатели по товарам и категориям.
- Поведение пользователей: сессии, просмотры, события.
- Запросы изначально экспортированы из Metabase для ClickHouse, но их можно использовать
  в любом BI-инструменте или Python/Jupyter Notebook с подключением к соответствующей базе.
- При использовании в другой СУБД могут потребоваться небольшие правки синтаксиса 
  (например, функции `FINAL` или `toStartOfDay` специфичны для ClickHouse).
"""

# -----------------------------
# KPI
# -----------------------------

# Выручка
REVENUE_KPI = """
SELECT
  SUM(`marts`.`daily_summary`.`revenue`) AS `sum`
FROM
  `marts`.`daily_summary` FINAL
[[ WHERE {{event_date}} ]]
"""

# Заказы
ORDERS_KPI = """
SELECT
  SUM(`marts`.`daily_summary`.`orders_count`) AS `sum`
FROM
  `marts`.`daily_summary` FINAL
[[ WHERE {{event_date}} ]]
"""

# Конверсия
CONVERSION_KPI = """
SELECT
  AVG(`marts`.`daily_summary`.`conversion_rate`) AS `avg`
FROM
  `marts`.`daily_summary` FINAL
[[ WHERE {{event_date}} ]]
"""

# Средний чек
AVG_CHECK_KPI = """
SELECT
  AVG(`marts`.`daily_summary`.`avg_check`) AS `avg`
FROM
  `marts`.`daily_summary` FINAL
[[ WHERE {{event_date}} ]]
"""

# -----------------------------
# Динамика по дням
# -----------------------------

# Выручка по дням
REVENUE_DAILY = """
SELECT
SELECT
  toStartOfDay(`marts`.`daily_summary`.`event_date`) AS `event_date`,
  SUM(`marts`.`daily_summary`.`revenue`) AS `sum`
FROM
  `marts`.`daily_summary` FINAL
[[ WHERE {{event_date}} ]]
GROUP BY
  toStartOfDay(`marts`.`daily_summary`.`event_date`)
ORDER BY
  `event_date` ASC
"""

# Количество заказов по дням
ORDERS_DAILY = """
SELECT
  toStartOfDay(`marts`.`daily_summary`.`event_date`) AS `event_date`,
  SUM(`marts`.`daily_summary`.`orders_count`) AS `sum`
FROM
  `marts`.`daily_summary` FINAL
[[ WHERE {{event_date}} ]]
GROUP BY
  toStartOfDay(`marts`.`daily_summary`.`event_date`)
ORDER BY
  `event_date` ASC
"""

# Конверсия по дням
CONVERSION_DAILY = """
SELECT
  toStartOfDay(`marts`.`daily_summary`.`event_date`) AS `event_date`,
  AVG(`marts`.`daily_summary`.`conversion_rate`) AS `avg`
FROM
  `marts`.`daily_summary` FINAL
[[ WHERE {{event_date}} ]]
GROUP BY
  toStartOfDay(`marts`.`daily_summary`.`event_date`)
ORDER BY
  `event_date` ASC
"""

# Уникальные пользователи по дням
UNIQUE_USERS_DAILY = """
SELECT
  toStartOfDay(`marts`.`daily_summary`.`event_date`) AS `event_date`,
  SUM(`marts`.`daily_summary`.`unique_users`) AS `sum`
FROM
  `marts`.`daily_summary` FINAL
[[ WHERE {{event_date}} ]]
GROUP BY
  toStartOfDay(`marts`.`daily_summary`.`event_date`)
ORDER BY
  `event_date` ASC
"""

# Средний чек по дням
AVG_CHECK_DAILY = """
SELECT
  toStartOfDay(`marts`.`daily_summary`.`event_date`) AS `event_date`,
  AVG(`marts`.`daily_summary`.`avg_check`) AS `avg`
FROM
  `marts`.`daily_summary` FINAL
[[ WHERE {{event_date}} ]]
GROUP BY
  toStartOfDay(`marts`.`daily_summary`.`event_date`)
ORDER BY
  `event_date` ASC
"""

# -----------------------------
# Продукты и категории
# -----------------------------

# Выручка по категориям
REVENUE_BY_CATEGORY = """
SELECT
  `marts`.`product_stats`.`category_name` AS `category_name`,
  SUM(`marts`.`product_stats`.`total_revenue`) AS `sum`
FROM
  `marts`.`product_stats` FINAL
[[ WHERE `marts`.`product_stats`.`category_name` = {{category_filter}} ]]
GROUP BY
  `marts`.`product_stats`.`category_name`
ORDER BY
  `marts`.`product_stats`.`category_name` ASC
"""

# Продажи по категориям
SALES_BY_CATEGORY = """
SELECT
  `marts`.`product_stats`.`category_name` AS `category_name`,
  SUM(`marts`.`product_stats`.`total_sold`) AS `sum`
FROM
  `marts`.`product_stats` FINAL
WHERE 1=1
[[ AND `marts`.`product_stats`.`category_name` = {{category_filter}} ]]
GROUP BY
  `marts`.`product_stats`.`category_name`
ORDER BY
  `marts`.`product_stats`.`category_name` ASC
"""

# Средняя цена товаров по категориям
AVG_PRICE_BY_CATEGORY = """
SELECT
  `marts`.`product_stats`.`category_name` AS `category_name`,
  AVG(`marts`.`product_stats`.`avg_price`) AS `avg`
FROM
  `marts`.`product_stats` FINAL
WHERE 1=1
[[ AND `marts`.`product_stats`.`category_name` = {{category_filter}} ]]
GROUP BY
  `marts`.`product_stats`.`category_name`
ORDER BY
  `marts`.`product_stats`.`category_name` ASC
"""

# Количество заказов с товаром
ORDERS_PER_PRODUCT = """
SELECT
  `marts`.`product_stats`.`product_name` AS `product_name`,
  SUM(`marts`.`product_stats`.`orders_count`) AS `sum`
FROM
  `marts`.`product_stats` FINAL
WHERE 1=1
[[ AND `marts`.`product_stats`.`category_name` = {{category_filter}} ]]
[[ AND `marts`.`product_stats`.`product_name` = {{product_filter}} ]]
GROUP BY
  `marts`.`product_stats`.`product_name`
ORDER BY
  `marts`.`product_stats`.`product_name` ASC
"""

# Топ-10 товаров по выручке
TOP10_PRODUCTS_REVENUE = """
SELECT
  `marts`.`product_stats`.`product_name` AS `product_name`,
  SUM(`marts`.`product_stats`.`total_revenue`) AS `sum`
FROM
  `marts`.`product_stats` FINAL
WHERE 1=1
[[ AND `marts`.`product_stats`.`category_name` = {{category_filter}} ]]
[[ AND `marts`.`product_stats`.`product_name` = {{product_filter}} ]]
GROUP BY
  `marts`.`product_stats`.`product_name`
ORDER BY
  `sum` DESC,
  `marts`.`product_stats`.`product_name` ASC
LIMIT
  10

"""

# -----------------------------
# Поведение пользователей
# -----------------------------

# Количество сессий по пользователям
SESSIONS_PER_USER = """
SELECT
  (
    FLOOR(
      (
        (`marts`.`user_behavior`.`sessions_count` - 0.75) / 0.75
      )
    ) * 0.75
  ) + 0.75 AS `sessions_count`,
  COUNT(*) AS `count`
FROM
  `marts`.`user_behavior` FINAL
GROUP BY
  (
    FLOOR(
      (
        (`marts`.`user_behavior`.`sessions_count` - 0.75) / 0.75
      )
    ) * 0.75
  ) + 0.75
ORDER BY
  (
    FLOOR(
      (
        (`marts`.`user_behavior`.`sessions_count` - 0.75) / 0.75
      )
    ) * 0.75
  ) + 0.75 ASC
"""

# Средняя длительность сессии
AVG_SESSION_DURATION = """
SELECT
  FLOOR(
    (
      `marts`.`user_behavior`.`avg_session_duration` / 5.0
    )
  ) * 5.0 AS `avg_session_duration`,
  COUNT(*) AS `count`
FROM
  `marts`.`user_behavior` FINAL
GROUP BY
  FLOOR(
    (
      `marts`.`user_behavior`.`avg_session_duration` / 5.0
    )
  ) * 5.0
ORDER BY
  FLOOR(
    (
      `marts`.`user_behavior`.`avg_session_duration` / 5.0
    )
  ) * 5.0 ASC
"""

# Количество просмотренных страниц за сессию
AVG_PAGES_PER_SESSION = """
SELECT
  FLOOR(
    (
      `marts`.`user_behavior`.`avg_pages_per_session` / 1.25
    )
  ) * 1.25 AS `avg_pages_per_session`,
  COUNT(*) AS `count`
FROM
  `marts`.`user_behavior` FINAL
GROUP BY
  FLOOR(
    (
      `marts`.`user_behavior`.`avg_pages_per_session` / 1.25
    )
  ) * 1.25
ORDER BY
  FLOOR(
    (
      `marts`.`user_behavior`.`avg_pages_per_session` / 1.25
    )
  ) * 1.25 ASC
"""

# Количество событий на пользователя
EVENTS_PER_USER = """
SELECT
  FLOOR((`marts`.`user_behavior`.`events_count` / 5.0)) * 5.0 AS `events_count`,
  COUNT(*) AS `count`
FROM
  `marts`.`user_behavior` FINAL
GROUP BY
  FLOOR((`marts`.`user_behavior`.`events_count` / 5.0)) * 5.0
ORDER BY
  FLOOR((`marts`.`user_behavior`.`events_count` / 5.0)) * 5.0 ASC
"""
