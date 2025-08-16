"""
Модуль для загрузки агрегированных витрин marts из DDS (Postgres) в ClickHouse.

Содержит функции для:
- получения клиента ClickHouse через Airflow Connection,
- создания таблиц marts в ClickHouse,
- загрузки данных из DDS (Postgres) и вставки в ClickHouse с безопасной конвертацией типов,
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


def load_clickhouse_marts(
    clickhouse_conn_id: str = "ClickHouse",
    postgres_conn_id: str = "Postgres"
) -> None:
    """
    Загружает агрегированные витрины marts из DDS (Postgres) в ClickHouse с логированием.

    Выполняет:
    - Создание таблиц marts (если они ещё не созданы)
    - Загрузку ежедневной сводки, статистики товаров и поведения пользователей
    - Конвертацию числовых типов и добавление версии для ReplacingMergeTree
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

        # Генерация версии для текущего запуска DAG
        version = int(datetime.now().timestamp() * 1000)

        # Загрузка ежедневной сводки
        logging.info("Загрузка ежедневной сводки из DDS...")
        data_daily = pg_hook.get_records(INSERT_DAILY_SUMMARY_SQL["source"])
        data_daily = [convert_numeric_row(row) for row in data_daily]
        data_daily = add_version(data_daily, version)
        client.execute(INSERT_DAILY_SUMMARY_SQL["insert"], data_daily)
        logging.info(f"Ежедневная сводка загружена: {len(data_daily)} строк")
        notify_telegram(f"Ежедневная сводка загружена: {len(data_daily)} строк")

        # Загрузка статистики товаров
        logging.info("Загрузка статистики товаров из DDS...")
        data_products = pg_hook.get_records(INSERT_PRODUCT_STATS_SQL["source"])
        data_products = [convert_numeric_row(row) for row in data_products]
        data_products = add_version(data_products, version)
        client.execute(INSERT_PRODUCT_STATS_SQL["insert"], data_products)
        logging.info(f"Статистика товаров загружена: {len(data_products)} строк")
        notify_telegram(f"Статистика товаров загружена: {len(data_products)} строк")

        # Загрузка поведения пользователей
        logging.info("Загрузка поведения пользователей из DDS...")
        data_users = pg_hook.get_records(INSERT_USER_BEHAVIOR_SQL["source"])
        data_users = [convert_numeric_row(row) for row in data_users]
        data_users = add_version(data_users, version)
        client.execute(INSERT_USER_BEHAVIOR_SQL["insert"], data_users)
        logging.info(f"Данные поведения пользователей загружены: {len(data_users)} строк")
        notify_telegram(f"Данные поведения пользователей загружены: {len(data_users)} строк")

        elapsed = round(time.time() - start_time, 2)
        logging.info(f"[v] DAG load_clickhouse_from_dds завершён за {elapsed} секунд")
        notify_telegram(f"[v] DAG load_clickhouse_from_dds завершён. Время выполнения: {elapsed} сек")

    except Exception as e:
        logging.error(f"[x] Ошибка в DAG load_clickhouse_from_dds: {e}", exc_info=True)
        notify_telegram(f"[x] Ошибка в DAG load_clickhouse_from_dds: {e}")
        raise


if __name__ == "__main__":
    pass
