"""
Модуль для загрузки агрегированных витрин marts из DDS (Postgres) в ClickHouse.

Содержит функции для:
- получения клиента ClickHouse через Airflow Connection,
- создания таблиц marts в ClickHouse,
- загрузки данных из DDS (Postgres) и вставки в ClickHouse,
- логирования ключевых этапов и ошибок в лог и Telegram.

Использует Airflow хуки для подключения к ClickHouse и Postgres
"""

import logging

from clickhouse_driver import Client
from airflow.hooks.base import BaseHook
from airflow.providers.postgres.hooks.postgres import PostgresHook

from utils.sql_db.sql_paths import (
    CREATE_MARTS_TABLES_SQL,
    INSERT_DAILY_SUMMARY_SQL,
    INSERT_PRODUCT_STATS_SQL,
    INSERT_USER_BEHAVIOR_SQL
)


def get_clickhouse_client(conn_id: str = "ClickHouse") -> Client:
    """
    Получает клиент ClickHouse по connection ID из Airflow Connections.

    :param conn_id: Идентификатор соединения в Airflow (default: "ClickHouse").
    :return: Клиент ClickHouse.
    :raises Exception: При ошибке подключения к ClickHouse.
    """
    try:
        logging.info(f"Получаем соединение ClickHouse с conn_id={conn_id}")
        conn = BaseHook.get_connection(conn_id)
        client = Client(
            host=conn.host,
            port=conn.port,
            user=conn.login,
            password=conn.password,
            database=conn.schema
        )
        logging.info("Соединение с ClickHouse установлено успешно")
        return client
    except Exception as e:
        logging.error(f"Ошибка при подключении к ClickHouse: {e}")
        raise


def load_clickhouse_marts(
    clickhouse_conn_id: str = "ClickHouse",
    postgres_conn_id: str = "Postgres"
) -> None:
    """
    Загружает агрегированные данные из DDS (Postgres) в ClickHouse витрины.

    :param clickhouse_conn_id: Airflow connection ID для ClickHouse.
    :param postgres_conn_id: Airflow connection ID для Postgres.
    :return: None
    :raises Exception: При ошибках на любом этапе загрузки.
    """
    logging.info("Начинаем загрузку marts в ClickHouse")

    client = get_clickhouse_client(clickhouse_conn_id)

    logging.info("Создание таблиц marts (если не существуют)...")
    for query in CREATE_MARTS_TABLES_SQL:
        client.execute(query)
    logging.info("Таблицы marts успешно созданы/проверены")

    pg_hook = PostgresHook(postgres_conn_id=postgres_conn_id)

    logging.info("Загрузка ежедневной сводки из DDS...")
    data_daily = pg_hook.get_records(INSERT_DAILY_SUMMARY_SQL["source"])
    client.execute(INSERT_DAILY_SUMMARY_SQL["insert"], data_daily)
    logging.info(f"Ежедневная сводка загружена: {len(data_daily)} строк")

    logging.info("Загрузка статистики товаров из DDS...")
    data_products = pg_hook.get_records(INSERT_PRODUCT_STATS_SQL["source"])
    client.execute(INSERT_PRODUCT_STATS_SQL["insert"], data_products)
    logging.info(f"Статистика товаров загружена: {len(data_products)} строк")

    logging.info("Загрузка поведения пользователей из DDS...")
    data_users = pg_hook.get_records(INSERT_USER_BEHAVIOR_SQL["source"])
    client.execute(INSERT_USER_BEHAVIOR_SQL["insert"], data_users)
    logging.info(f"Данные поведения пользователей загружены: {len(data_users)} строк")

    logging.info("[v] Загрузка витрин marts в ClickHouse завершена")


if __name__ == "__main__":
    pass
