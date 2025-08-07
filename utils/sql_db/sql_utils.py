"""
Утилиты для чтения SQL-файлов и выполнения запросов к базе данных PostgreSQL.

Содержит универсальные функции для выполнения SELECT, INSERT, UPDATE и DELETE запросов,
а также специализированную функцию для получения новых сырых событий.
"""

import logging
from typing import Optional, Tuple, List
from airflow.providers.postgres.hooks.postgres import PostgresHook


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


def run_select_query(pg_hook: PostgresHook, sql_path: str, params: Optional[Tuple] = None) -> List[Tuple]:
    """
    Универсальная функция для выполнения SELECT-запроса и получения результатов.

    :param pg_hook: PostgresHook с соединением.
    :param sql_path: Путь к SQL-файлу с SELECT-запросом.
    :param params: Параметры запроса (если нужны).
    :return: Список кортежей с результатами запроса.
    """
    try:
        sql = read_sql_file(sql_path)
        if params:
            return pg_hook.get_records(sql, parameters=params)
        return pg_hook.get_records(sql)
    except Exception as e:
        logging.error(f"Ошибка при выполнении SELECT из файла {sql_path} с параметрами {params}: {e}")
        raise


def run_modify_query(pg_hook: PostgresHook, sql_path: str, params: Optional[Tuple] = None) -> None:
    """
    Универсальная функция для выполнения INSERT/UPDATE/DELETE-запросов.

    :param pg_hook: PostgresHook с соединением.
    :param sql_path: Путь к SQL-файлу.
    :param params: Параметры для запроса.
    """
    try:
        sql = read_sql_file(sql_path)
        pg_hook.run(sql, parameters=params, autocommit=True)
    except Exception as e:
        logging.error(f"Ошибка при выполнении запроса из файла {sql_path} с параметрами {params}: {e}")
        raise


def run_select_with_columns(pg_hook: PostgresHook, sql_path: str, params: Optional[Tuple] = None) -> Tuple[List[Tuple], List[str]]:
    """
    Универсальная функция для выполнения SELECT-запроса с возвратом данных и названий колонок.

    :param pg_hook: PostgresHook с соединением.
    :param sql_path: Путь к SQL-файлу.
    :param params: Параметры запроса (если нужны).
    :return: Кортеж из двух элементов:
        - Список кортежей с результатами.
        - Список названий колонок.
    """
    try:
        sql = read_sql_file(sql_path)
        conn = pg_hook.get_conn()
        with conn.cursor() as cursor:
            cursor.execute(sql, params)
            rows = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
        return rows, columns
    except Exception as e:
        logging.error(f"Ошибка при выполнении SELECT из файла {sql_path} с параметрами {params}: {e}")
        raise



