"""
Модуль для работы с таблицей учёта обработанных файлов в RAW-слое.

Содержит функции для получения списка уже обработанных файлов,
отметки файла как обработанного, а также создания таблицы raw.processed_events.

Используется для предотвращения повторной обработки одних и тех же файлов.
"""

from typing import Set
from airflow.providers.postgres.hooks.postgres import PostgresHook

from utils.sql_db.sql_utils import run_select_query, run_modify_query
from utils.sql_db.schema_and_tables_init import create_table_from_file
from utils.sql_db.sql_paths import (
    SQL_SELECT_PROCESSED_FILES,
    SQL_MARK_PROCESSED,
    SQL_CREATE_PROCESSED_EVENTS
)

def get_processed_files(pg_hook: PostgresHook) -> Set[str]:
    """
    Получает множество имён уже обработанных файлов из таблицы raw.processed_files.

    :param pg_hook: Экземпляр PostgresHook с активным соединением.
    :return: Множество строк — имена обработанных файлов.
    :raises Exception: Если запрос к БД завершился с ошибкой.
    """
    records = run_select_query(pg_hook, SQL_SELECT_PROCESSED_FILES)
    return {r[0] for r in records}


def mark_file_as_processed(pg_hook: PostgresHook, filename: str) -> None:
    """
    Добавляет имя обработанного файла в таблицу raw.processed_files.

    :param pg_hook: Экземпляр PostgresHook с активным соединением.
    :param filename: Имя файла, который был успешно обработан.
    :return: None
    :raises Exception: Если вставка в таблицу завершилась с ошибкой.
    """
    run_modify_query(pg_hook, SQL_MARK_PROCESSED, params=(filename,))


def create_processed_events_table(pg_hook: PostgresHook) -> None:
    """
    Создаёт таблицу raw.processed_events, если она ещё не существует.

    :param pg_hook: Экземпляр PostgresHook с активным соединением.
    :return: None
    """
    create_table_from_file(pg_hook, SQL_CREATE_PROCESSED_EVENTS, 'raw.processed_events')