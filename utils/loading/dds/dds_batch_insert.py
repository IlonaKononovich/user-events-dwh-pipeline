"""
Модуль dds_batch_insert

Содержит функцию batch_insert для выполнения пакетной вставки данных в PostgreSQL с использованием Airflow PostgresHook.
"""

import logging
from typing import Any, Dict, List
from airflow.providers.postgres.hooks.postgres import PostgresHook


def batch_insert(pg: PostgresHook, sql: str, data: List[Dict[str, Any]]) -> None:
    """
    Выполнить пакетную вставку данных в БД PostgreSQL через Airflow Hook.

    :param pg: Объект PostgresHook для подключения к базе данных.
    :param sql: SQL-запрос с плейсхолдерами (%s) для вставки значений.
    :param data: Список словарей, где каждый словарь — строка данных. Ключи — названия колонок.
    :raises Exception: При ошибке выполнения запроса (например, нарушении ограничений или недоступности БД).
    :return: None
    """
    if not data:
        logging.debug("batch_insert: данных для вставки нет")
        return
    conn = pg.get_conn()
    try:
        with conn.cursor() as cur:
            columns = list(data[0].keys())
            values = [tuple(d[c] for c in columns) for d in data]
            logging.debug(f"batch_insert: Выполняется вставка {len(data)} записей с колонками {columns}")
            cur.executemany(sql, values)
        conn.commit()
        logging.info(f"batch_insert: успешно вставлено {len(data)} записей")
    except Exception as e:
        logging.error(f"batch_insert: ошибка вставки: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    pass