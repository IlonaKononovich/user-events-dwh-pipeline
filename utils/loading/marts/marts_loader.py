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
import time

from clickhouse_driver import Client
from airflow.hooks.base import BaseHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from utils.telegram_logger import notify_telegram

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
    Загружает агрегированные витрины marts из DDS (Postgres) в ClickHouse с логированием.

    Выполняет:
    - Создание таблиц marts (если они ещё не созданы)
    - Загрузку ежедневной сводки, статистики товаров и поведения пользователей
    - Логирование количества обработанных строк по каждой витрине
    - Отправку уведомлений в Telegram о старте, прогрессе и завершении
    - Подсчёт времени выполнения DAG

    :param clickhouse_conn_id: Airflow connection ID для ClickHouse (по умолчанию "ClickHouse")
    :param postgres_conn_id: Airflow connection ID для Postgres (по умолчанию "Postgres")
    :return: None
    :raises Exception: Пробрасывает ошибки подключения или вставки данных
    """
    start_time = time.time()
    try:
        notify_telegram("DAG load_clickhouse_from_dds запущен")
        logging.info("Начинаем загрузку marts в ClickHouse")

        client = get_clickhouse_client(clickhouse_conn_id)

        # Создание таблиц
        logging.info("Создание таблиц marts (если не существуют)...")
        for query in CREATE_MARTS_TABLES_SQL:
            client.execute(query)
        logging.info("Таблицы marts успешно созданы/проверены")

        pg_hook = PostgresHook(postgres_conn_id=postgres_conn_id)

        # Загрузка ежедневной сводки
        logging.info("Загрузка ежедневной сводки из DDS...")
        data_daily = pg_hook.get_records(INSERT_DAILY_SUMMARY_SQL["source"])
        client.execute(INSERT_DAILY_SUMMARY_SQL["insert"], data_daily)
        msg = f"Ежедневная сводка загружена: {len(data_daily)} строк"
        logging.info(msg)
        notify_telegram(msg)

        # Загрузка статистики товаров
        logging.info("Загрузка статистики товаров из DDS...")
        data_products = pg_hook.get_records(INSERT_PRODUCT_STATS_SQL["source"])
        client.execute(INSERT_PRODUCT_STATS_SQL["insert"], data_products)
        msg = f"Статистика товаров загружена: {len(data_products)} строк"
        logging.info(msg)
        notify_telegram(msg)

        # Загрузка поведения пользователей
        logging.info("Загрузка поведения пользователей из DDS...")
        data_users = pg_hook.get_records(INSERT_USER_BEHAVIOR_SQL["source"])
        client.execute(INSERT_USER_BEHAVIOR_SQL["insert"], data_users)
        msg = f"Данные поведения пользователей загружены: {len(data_users)} строк"
        logging.info(msg)
        notify_telegram(msg)

        elapsed = round(time.time() - start_time, 2)
        logging.info(f"[v] DAG load_clickhouse_from_dds завершён за {elapsed} секунд")
        notify_telegram(f"[v] DAG load_clickhouse_from_dds завершён. Время выполнения: {elapsed} сек")

    except Exception as e:
        logging.error(f"[x] Ошибка в DAG load_clickhouse_from_dds: {e}", exc_info=True)
        notify_telegram(f"[x] Ошибка в DAG load_clickhouse_from_dds: {e}")
        raise


if __name__ == "__main__":
    pass
