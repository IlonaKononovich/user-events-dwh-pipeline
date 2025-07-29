import os
import logging
from typing import Set
from airflow.providers.postgres.hooks.postgres import PostgresHook


# Пути к SQL-скриптам
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))

BASE_SQL_RAW_CREATE = os.path.join(BASE_DIR, 'sql', 'raw', 'create')
BASE_SQL_RAW_INSERT = os.path.join(BASE_DIR, 'sql', 'raw', 'insert')

SQL_CREATE_EVENTS = os.path.join(BASE_SQL_RAW_CREATE, 'create_raw_events.sql')
SQL_CREATE_PROCESSED = os.path.join(BASE_SQL_RAW_CREATE, 'create_processed_files.sql')

SQL_MARK_PROCESSED = os.path.join(BASE_SQL_RAW_INSERT, 'mark_file_processed.sql')


def read_sql_file(path: str) -> str:
    """
    Читает SQL-скрипт из файла.

    :param path: Путь до .sql-файла
    :return: Строка с SQL-кодом
    """
    try:
        with open(path, 'r') as f:
            return f.read()
    except Exception as e:
        logging.error(f"Ошибка при чтении SQL-файла {path}: {e}")
        raise


def get_processed_files(pg_hook: PostgresHook) -> Set[str]:
    """
    Получает множество имён уже обработанных файлов из таблицы raw.processed_files.

    :param pg_hook: Инстанс PostgresHook с активным соединением
    :return: Множество имён файлов
    """
    try:
        sql = "SELECT filename FROM raw.processed_files"
        return {r[0] for r in pg_hook.get_records(sql)}
    except Exception as e:
        logging.error(f"Ошибка при получении обработанных файлов: {e}")
        raise


def mark_file_as_processed(pg_hook: PostgresHook, filename: str) -> None:
    """
    Добавляет имя обработанного файла в таблицу raw.processed_files.

    :param pg_hook: Инстанс PostgresHook
    :param filename: Имя файла, который был успешно обработан
    """
    try:
        sql = read_sql_file(SQL_MARK_PROCESSED)
        pg_hook.run(sql, parameters=(filename,))
    except Exception as e:
        logging.error(f"Ошибка при отметке файла как обработанного ({filename}): {e}")
        raise


def create_raw_events_table(pg_hook: PostgresHook) -> None:
    """
    Создаёт таблицу raw.events, если она ещё не существует.
    """
    try:
        pg_hook.run(read_sql_file(SQL_CREATE_EVENTS))
        logging.info("Таблица raw.events проверена/создана.")
    except Exception as e:
        logging.error(f"Ошибка при создании таблицы raw.events: {e}")
        raise


def create_processed_files_table(pg_hook: PostgresHook) -> None:
    """
    Создаёт таблицу raw.processed_files, если она ещё не существует.
    """
    try:
        pg_hook.run(read_sql_file(SQL_CREATE_PROCESSED))
        logging.info("Таблица raw.processed_files проверена/создана.")
    except Exception as e:
        logging.error(f"Ошибка при создании таблицы raw.processed_files: {e}")
        raise


if __name__ == "__main__":
    pass