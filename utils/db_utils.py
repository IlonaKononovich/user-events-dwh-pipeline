import os
import logging
from typing import Set
from airflow.providers.postgres.hooks.postgres import PostgresHook
from typing import Set

# Пути к SQL-скриптам
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))

BASE_SQL_RAW_CREATE = os.path.join(BASE_DIR, 'sql', 'raw', 'create')
BASE_SQL_RAW_INSERT = os.path.join(BASE_DIR, 'sql', 'raw', 'insert')
BASE_SQL_RAW_SELECT = os.path.join(BASE_DIR, 'sql', 'raw', 'select')

BASE_SQL_DDS_CREATE = os.path.join(BASE_DIR, 'sql', 'dds', 'create')

SQL_CREATE_RAW_SCHEMA = os.path.join(BASE_SQL_RAW_CREATE, 'create_raw_schema.sql')
SQL_CREATE_EVENTS = os.path.join(BASE_SQL_RAW_CREATE, 'create_raw_events.sql')
SQL_CREATE_PROCESSED = os.path.join(BASE_SQL_RAW_CREATE, 'create_raw_processed_files.sql')
SQL_CREATE_PROCESSED_EVENTS = os.path.join(BASE_SQL_RAW_CREATE, 'create_raw_processed_events.sql')
SQL_CREATE_EVENTS_INVALID = os.path.join(BASE_SQL_RAW_CREATE, 'create_raw_events_invalid.sql')

SQL_CREATE_DDS_SCHEMA = os.path.join(BASE_SQL_DDS_CREATE, 'create_dds_schema.sql')

SQL_MARK_PROCESSED = os.path.join(BASE_SQL_RAW_INSERT, 'mark_file_processed.sql')
SQL_SELECT_PROCESSED_FILES = os.path.join(BASE_SQL_RAW_SELECT, 'select_processed_files.sql')




def read_sql_file(path: str) -> str:
    """
    Читает SQL-скрипт из файла и возвращает его содержимое в виде строки.

    :param path: Абсолютный путь до SQL-файла.
    :return: Строка с SQL-кодом.
    :raises Exception: Если файл не удаётся прочитать.
    """
    try:
        with open(path, 'r', encoding='utf-8') as f:
            return f.read()
    except Exception as e:
        logging.error(f"Ошибка при чтении SQL-файла {path}: {e}")
        raise


def create_raw_schema(pg_hook: PostgresHook) -> None:
    """
    Создаёт схему raw, если она ещё не существует.

    :param pg_hook: Экземпляр PostgresHook с активным соединением.
    :return: None
    :raises Exception: Если выполнение SQL завершается с ошибкой.
    """
    try:
        pg_hook.run(read_sql_file(SQL_CREATE_RAW_SCHEMA), autocommit=True)
        logging.info("Схема raw проверена/создана.")
    except Exception as e:
        logging.error(f"Ошибка при создании схемы raw: {e}")
        raise


def create_raw_events_table(pg_hook: PostgresHook) -> None:
    """
    Создаёт таблицу raw.events, если она ещё не существует.

    :param pg_hook: Экземпляр PostgresHook с активным соединением.
    :return: None
    :raises Exception: Если выполнение SQL завершается с ошибкой.
    """
    try:
        pg_hook.run(read_sql_file(SQL_CREATE_EVENTS), autocommit=True)
        logging.info("Таблица raw.events проверена/создана.")
    except Exception as e:
        logging.error(f"Ошибка при создании таблицы raw.events: {e}")
        raise


def create_raw_processed_files_table(pg_hook: PostgresHook) -> None:
    """
    Создаёт таблицу raw.processed_files, если она ещё не существует.

    :param pg_hook: Экземпляр PostgresHook с активным соединением.
    :return: None
    :raises Exception: Если выполнение SQL завершается с ошибкой.
    """
    try:
        pg_hook.run(read_sql_file(SQL_CREATE_PROCESSED), autocommit=True)
        logging.info("Таблица raw.processed_files проверена/создана.")
    except Exception as e:
        logging.error(f"Ошибка при создании таблицы raw.processed_files: {e}")
        raise


def create_raw_events_invalid_table(pg_hook: PostgresHook) -> None:
    """
    Создаёт таблицу raw.events_invalid, если она ещё не существует.

    :param pg_hook: Экземпляр PostgresHook с активным соединением.
    :return: None
    :raises Exception: Если выполнение SQL завершается с ошибкой.
    """
    try:
        pg_hook.run(read_sql_file(SQL_CREATE_EVENTS_INVALID), autocommit=True)
        logging.info("Таблица raw.events_invalid проверена/создана.")
    except Exception as e:
        logging.error(f"Ошибка при создании таблицы raw.events_invalid: {e}")
        raise


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


def get_processed_files(pg_hook: PostgresHook) -> Set[str]:
    """
    Получает множество имён уже обработанных файлов из таблицы raw.processed_files.

    :param pg_hook: Экземпляр PostgresHook с активным соединением.
    :return: Множество строк — имена обработанных файлов.
    :raises Exception: Если запрос к БД завершился с ошибкой.
    """
    try:
        sql = read_sql_file(SQL_SELECT_PROCESSED_FILES)
        return {r[0] for r in pg_hook.get_records(sql)}
    except Exception as e:
        logging.error(f"Ошибка при получении обработанных файлов: {e}")
        raise


def mark_file_as_processed(pg_hook: PostgresHook, filename: str) -> None:
    """
    Добавляет имя обработанного файла в таблицу raw.processed_files.

    :param pg_hook: Экземпляр PostgresHook с активным соединением.
    :param filename: Имя файла, который был успешно обработан.
    :return: None
    :raises Exception: Если вставка в таблицу завершилась с ошибкой.
    """
    try:
        sql = read_sql_file(SQL_MARK_PROCESSED)
        pg_hook.run(sql, parameters=(filename,), autocommit=True)
    except Exception as e:
        logging.error(f"Ошибка при отметке файла как обработанного ({filename}): {e}")
        raise


def create_processed_events_table(pg_hook: PostgresHook) -> None:
    """
    Создаёт таблицу raw.processed_events, если она ещё не существует.

    :param pg_hook: Экземпляр PostgresHook с активным соединением.
    :return: None
    :raises Exception: Если выполнение SQL завершается с ошибкой.
    """
    try:
        pg_hook.run(read_sql_file(SQL_CREATE_PROCESSED_EVENTS), autocommit=True)
        logging.info("Таблица raw.processed_events проверена/создана.")
    except Exception as e:
        logging.error(f"Ошибка при создании таблицы raw.processed_events: {e}")
        raise

def create_dds_schema(pg_hook: PostgresHook) -> None:
    """
    Создаёт схему dds, если она ещё не существует.

    :param pg_hook: Экземпляр PostgresHook с активным соединением.
    :return: None
    :raises Exception: Если выполнение SQL завершается с ошибкой.
    """
    try:
        pg_hook.run(read_sql_file(SQL_CREATE_DDS_SCHEMA), autocommit=True)
        logging.info("Схема dds проверена/создана.")
    except Exception as e:
        logging.error(f"Ошибка при создании схемы dds: {e}")
        raise

def create_dds_tables(pg_hook: PostgresHook) -> None:
    """
    Создаёт все таблицы DDS, если они ещё не существуют.

    :param pg_hook: Экземпляр PostgresHook с активным соединением.
    :return: None
    :raises Exception: Если выполнение SQL завершается с ошибкой.
    """
    try:
        create_scripts = sorted(
            f for f in os.listdir(BASE_SQL_DDS_CREATE)
            if f.endswith('.sql') and f != 'create_dds_schema.sql'
        )
        for script in create_scripts:
            path = os.path.join(BASE_SQL_DDS_CREATE, script)
            sql = read_sql_file(path)
            pg_hook.run(sql, autocommit=True)
            logging.info(f"Таблица по скрипту {script} проверена/создана.")
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


if __name__ == "__main__":
    pass