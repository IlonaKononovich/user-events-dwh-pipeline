"""
Модуль dds_batch_processing

Содержит функцию для обработки батча событий: подготовка, загрузка в DIM и FACT таблицы, отметка обработанных записей.
"""

import logging
from typing import Dict, List, Tuple
from airflow.providers.postgres.hooks.postgres import PostgresHook

from utils.loading.dds.dds_dim_processing import process_dim_tables, process_dim_session
from utils.loading.dds.dds_fact_processing import process_fact_tables


def process_batch(
    batch_rows: List[Tuple],
    columns: List[str],
    pg: PostgresHook,
    insert_dim_sql: Dict[str, str],
    insert_fact_sql: Dict[str, str],
    mark_processed_sql: str
) -> Tuple[int, List[str]]:
    """
    Обработать батч событий из слоя staging:
    - Преобразует кортежи в словари
    - Загружает данные в DIM и FACT таблицы
    - Отмечает успешно загруженные события как обработанные

    :param batch_rows: Список кортежей из staging-таблицы (одна строка — одно событие).
    :param columns: Список названий колонок, соответствующих порядку в batch_rows.
    :param pg: Экземпляр PostgresHook для подключения к базе данных.
    :param insert_dim_sql: Словарь SQL-запросов для вставки в dimension таблицы.
    :param insert_fact_sql: Словарь SQL-запросов для вставки в факт таблицы.
    :param mark_processed_sql: SQL-запрос для отметки событий как обработанных (по stg_id).
    :return: Кортеж из двух элементов:
        - количество успешно обработанных событий
        - список event_id, вызвавших ошибки
    """
    batch_dicts = [dict(zip(columns, row)) for row in batch_rows]

    errors_dim = process_dim_tables(pg, batch_dicts, insert_dim_sql)
    errors_session = process_dim_session(pg, batch_dicts, insert_dim_sql)
    errors_fact = process_fact_tables(pg, batch_dicts, insert_fact_sql)

    error_ids = list(set(errors_dim + errors_session + errors_fact))

    processed_ids = [(d['stg_id'],) for d in batch_dicts if str(d.get('event_id')) not in error_ids]
    if processed_ids:
        conn = pg.get_conn()
        with conn.cursor() as cur:
            cur.executemany(mark_processed_sql, processed_ids)
        conn.commit()

    success_count = len(batch_dicts) - len(error_ids)
    logging.info(f"Батч обработан: успех={success_count}, ошибок={len(error_ids)}")
    return success_count, error_ids


if __name__ == "__main__":
    pass
