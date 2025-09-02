"""
Модуль для загрузки агрегированных витрин marts из DDS (Postgres) в ClickHouse.

Содержит функции для:
- получения клиента ClickHouse через Airflow Connection,
- создания таблиц marts в ClickHouse,
- загрузки ежедневной сводки, статистики товаров и поведения пользователей из DDS,
- безопасной конвертации типов данных для ClickHouse,
- логирования ключевых этапов и ошибок в лог и Telegram.

Использует Airflow хуки для подключения к ClickHouse и Postgres.
Версия данных генерируется в Python для предотвращения дублирования при повторных запусках DAG.
"""

import logging
import time
from datetime import datetime
from decimal import Decimal
from typing import List, Tuple, Any

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


def convert_numeric_row(row: Tuple[Any, ...], float_indices: List[int] = None) -> Tuple[Any, ...]:
    """
    Универсально конвертирует Decimal и None в float для безопасной вставки в ClickHouse.

    :param row: Кортеж с данными строки.
    :param float_indices: Список индексов колонок, которые нужно конвертировать. 
                          Если None — конвертируются все Decimal и None.
    :return: Кортеж с исправленными значениями.
    """
    fixed_row = list(row)
    if float_indices is None:
        for i, val in enumerate(fixed_row):
            if isinstance(val, Decimal):
                fixed_row[i] = float(val)
            elif val is None:
                fixed_row[i] = 0.0
    else:
        for i in float_indices:
            val = fixed_row[i]
            fixed_row[i] = float(val) if val is not None else 0.0
    return tuple(fixed_row)


def add_version(data: List[Tuple[Any, ...]], version: int) -> List[Tuple[Any, ...]]:
    """
    Добавляет колонку version ко всем строкам для вставки в ClickHouse.

    :param data: Список кортежей данных.
    :param version: Целочисленное значение версии для текущего запуска DAG.
    :return: Новый список кортежей с добавленной колонкой version.
    """
    return [tuple(list(row) + [version]) for row in data]


def create_ch_tables(clickhouse_conn_id: str = "ClickHouse") -> None:
    """
    Создаёт витрины в ClickHouse, если ещё не созданы.

    :param clickhouse_conn_id: Идентификатор соединения с ClickHouse в Airflow (default: "ClickHouse")
    :return: None
    """
    client = get_clickhouse_client(clickhouse_conn_id)
    for query in CREATE_MARTS_TABLES_SQL:
        client.execute(query)
    logging.info("Таблицы marts успешно созданы/проверены")


def load_mart_table(
    sql_source: str,
    sql_insert: str,
    description: str,
    clickhouse_conn_id: str = "ClickHouse",
    postgres_conn_id: str = "Postgres"
) -> None:
    """
    Универсальная функция загрузки витрины из DDS (Postgres) в ClickHouse.

    :param sql_source: SQL-запрос для извлечения данных из DDS
    :param sql_insert: SQL-запрос для вставки данных в ClickHouse
    :param description: Текстовое описание витрины (для логов и Telegram)
    :param clickhouse_conn_id: Идентификатор соединения с ClickHouse в Airflow (default: "ClickHouse")
    :param postgres_conn_id: Идентификатор соединения с Postgres в Airflow (default: "Postgres")
    :return: None
    """
    client = get_clickhouse_client(clickhouse_conn_id)
    pg_hook = PostgresHook(postgres_conn_id)
    version = int(datetime.now().timestamp() * 1000)

    data = pg_hook.get_records(sql_source)
    data = [convert_numeric_row(row) for row in data]
    data = add_version(data, version)

    client.execute(sql_insert, data)
    logging.info(f"{description} загружена: {len(data)} строк")
    notify_telegram(f"{description} загружена: {len(data)} строк")


def load_daily_summary_to_ch(clickhouse_conn_id: str = "ClickHouse", postgres_conn_id: str = "Postgres") -> None:
    """
    Загружает агрегированную ежедневную сводку из DDS (Postgres) в ClickHouse.

    :param clickhouse_conn_id: Идентификатор соединения с ClickHouse в Airflow (default: "ClickHouse")
    :param postgres_conn_id: Идентификатор соединения с Postgres в Airflow (default: "Postgres")
    :return: None
    """
    load_mart_table(
        sql_source=INSERT_DAILY_SUMMARY_SQL["source"],
        sql_insert=INSERT_DAILY_SUMMARY_SQL["insert"],
        description="Ежедневная сводка",
        clickhouse_conn_id=clickhouse_conn_id,
        postgres_conn_id=postgres_conn_id,
    )


def load_product_stats_to_ch(clickhouse_conn_id: str = "ClickHouse", postgres_conn_id: str = "Postgres") -> None:
    """
    Загружает статистику товаров из DDS (Postgres) в ClickHouse.

    :param clickhouse_conn_id: Идентификатор соединения с ClickHouse в Airflow (default: "ClickHouse")
    :param postgres_conn_id: Идентификатор соединения с Postgres в Airflow (default: "Postgres")
    :return: None
    """
    load_mart_table(
        sql_source=INSERT_PRODUCT_STATS_SQL["source"],
        sql_insert=INSERT_PRODUCT_STATS_SQL["insert"],
        description="Статистика товаров",
        clickhouse_conn_id=clickhouse_conn_id,
        postgres_conn_id=postgres_conn_id,
    )


def load_user_behavior_to_ch(clickhouse_conn_id: str = "ClickHouse", postgres_conn_id: str = "Postgres") -> None:
    """
    Загружает данные о поведении пользователей из DDS (Postgres) в ClickHouse.

    :param clickhouse_conn_id: Идентификатор соединения с ClickHouse в Airflow (default: "ClickHouse")
    :param postgres_conn_id: Идентификатор соединения с Postgres в Airflow (default: "Postgres")
    :return: None
    """
    load_mart_table(
        sql_source=INSERT_USER_BEHAVIOR_SQL["source"],
        sql_insert=INSERT_USER_BEHAVIOR_SQL["insert"],
        description="Поведение пользователей",
        clickhouse_conn_id=clickhouse_conn_id,
        postgres_conn_id=postgres_conn_id,
    )


if __name__ == "__main__":
    pass
