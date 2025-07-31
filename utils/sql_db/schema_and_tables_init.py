"""
Модуль инициализации схем и таблиц в базе данных.

Содержит функции для создания RAW и DDS схем и таблиц,
а также функции для инициализации слоёв данных.

Функции обеспечивают последовательное создание необходимых объектов
с логированием успешных операций и ошибок.
"""

import os
import logging

from airflow.providers.postgres.hooks.postgres import PostgresHook

from utils.sql_db.sql_utils import read_sql_file
from utils.sql_db.sql_paths import (
    SQL_CREATE_RAW_SCHEMA,
    SQL_CREATE_EVENTS,
    SQL_CREATE_PROCESSED,
    SQL_CREATE_EVENTS_INVALID,
    SQL_CREATE_DDS_SCHEMA,
)
from utils.constants import CREATE_DDS_ORDER
from utils.loading.raw_processed_files import create_processed_events_table


def create_schema(pg_hook: PostgresHook, schema_name: str, sql_path: str) -> None  :
    """
    Создаёт схему, если она ещё не существует.

    :param pg_hook: Экземпляр PostgresHook с активным соединением.
    :param schema_name: Имя схемы (для логирования).
    :param sql_path: Путь к SQL-файлу с CREATE SCHEMA.
    :return: None
    :raises Exception: При ошибке выполнения SQL.
    """
    try:
        pg_hook.run(read_sql_file(sql_path), autocommit=True)
        logging.info(f"Схема {schema_name} проверена/создана.")
    except Exception as e:
        logging.error(f"Ошибка при создании схемы {schema_name}: {e}")
        raise


def create_table_from_file(pg_hook: PostgresHook, sql_path: str, table_name: str) -> None:
    """
    Универсальная функция для создания таблицы из SQL-файла.

    :param pg_hook: Экземпляр PostgresHook с активным соединением.
    :param sql_path: Путь к SQL-файлу с CREATE TABLE.
    :param table_name: Имя таблицы для логирования.
    """
    try:
        sql = read_sql_file(sql_path)
        pg_hook.run(sql, autocommit=True)
        logging.info(f"Таблица {table_name} успешно создана или уже существует.")
    except Exception as e:
        logging.error(f"Ошибка при создании таблицы {table_name}: {e}")
        raise


def create_raw_schema(pg_hook: PostgresHook) -> None:
    """
    Создаёт схему raw в базе данных, если она ещё не существует.

    :param pg_hook: Экземпляр PostgresHook для выполнения SQL-запросов.
    :return: None
    """
    create_schema(pg_hook, 'raw', SQL_CREATE_RAW_SCHEMA)


def create_raw_events_table(pg_hook: PostgresHook) -> None:
    """
    Создаёт таблицу raw.events, если она ещё не существует.

    :param pg_hook: Экземпляр PostgresHook с активным соединением.
    :return: None
    """
    create_table_from_file(pg_hook, SQL_CREATE_EVENTS, 'raw.events')


def create_raw_processed_files_table(pg_hook: PostgresHook) -> None:
    """
    Создаёт таблицу raw.processed_files, если она ещё не существует.

    :param pg_hook: Экземпляр PostgresHook с активным соединением.
    :return: None
    """
    create_table_from_file(pg_hook, SQL_CREATE_PROCESSED, 'raw.processed_files')


def create_raw_events_invalid_table(pg_hook: PostgresHook) -> None:
    """
    Создаёт таблицу raw.events_invalid, если она ещё не существует.

    :param pg_hook: Экземпляр PostgresHook с активным соединением.
    :return: None
    """
    create_table_from_file(pg_hook, SQL_CREATE_EVENTS_INVALID, 'raw.events_invalid')


def init_raw_layer(pg_hook: PostgresHook) -> None:
    """
    Инициализация RAW-слоя: создаёт схему raw и обе необходимые таблицы
    (raw.events и raw.processed_files).

    :param pg_hook: Экземпляр PostgresHook с активным соединением.
    :return: None
    """
    create_raw_schema(pg_hook)
    create_raw_events_table(pg_hook)
    create_raw_processed_files_table(pg_hook)
    create_raw_events_invalid_table(pg_hook)
    logging.info("RAW-слой успешно инициализирован.")


def create_dds_schema(pg_hook: PostgresHook) -> None:
    """
    Создаёт схему dds в базе данных, если она ещё не существует.

    :param pg_hook: Экземпляр PostgresHook для выполнения SQL-запросов.
    :return: None
    """
    create_schema(pg_hook, 'dds', SQL_CREATE_DDS_SCHEMA)


def create_dds_tables(pg_hook: PostgresHook) -> None:
    """
    Создаёт все таблицы DDS в строго определённом порядке, чтобы избежать ошибок из-за FK.

    :param pg_hook: Экземпляр PostgresHook с активным соединением.
    :return: None
    :raises Exception: При ошибке выполнения SQL.
    """
    try:
        for script in CREATE_DDS_ORDER:
            path = os.path.join(BASE_SQL_DDS_CREATE, script)
            table_name = script.replace('create_', '').replace('.sql', '').replace('_', '.')
            create_table_from_file(pg_hook, path, table_name)
    except Exception as e:
        logging.error(f"Ошибка при создании таблиц DDS: {e}")
        raise


def init_dds_layer(pg_hook: PostgresHook) -> None:
    """
    Инициализация DDS-слоя: создаёт схему dds, все таблицы из папки sql/dds/create
    и таблицу raw.processed_events

    :param pg_hook: Экземпляр PostgresHook с активным соединением.
    :return: None
    """
    create_dds_schema(pg_hook)
    create_dds_tables(pg_hook)
    create_processed_events_table(pg_hook)
    logging.info("DDS-слой успешно инициализирован.")