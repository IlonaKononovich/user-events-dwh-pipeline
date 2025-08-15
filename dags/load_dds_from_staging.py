import time
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Tuple

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from utils.telegram_logger import notify_telegram
from utils.sql_db.sql_utils import read_sql_file
from utils.sql_db.schema_and_tables_init import init_dds_layer
from utils.loading.dds.dds_sql_loader import get_insert_dim_sql, get_insert_fact_sql
from utils.loading.dds.dds_batch_processing import process_batch
from utils.loading.dds.dds_sql_loader import fetch_new_staging_events
from utils.sql_db.sql_paths import SQL_MARK_PROCESSED_STAGING
from utils.constants import BATCH_SIZE_DDS


# Аргументы DAG по умолчанию
default_args = {
    'owner': 'ilona',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}


def init_dds_layer_wrapper() -> None:
    """
    Обёртка для инициализации DDS-слоя (схема + таблицы)
    :return: None
    """
    pg = PostgresHook(postgres_conn_id='Postgres')
    init_dds_layer(pg)


def load_staging_events_batch(
    pg: PostgresHook,
    insert_dim_sql: Dict[str, str],
    insert_fact_sql: Dict[str, str],
    mark_processed_sql: str,
) -> Tuple[int, List[str]]:
    """
    Основная функция загрузки батчей событий из staging.

    Загружает новые события, обрабатывает их по частям (батчам), 
    вызывает функции обработки DIM и FACT таблиц и отмечает успешно обработанные записи.

    :param pg: PostgresHook для подключения к базе.
    :param insert_dim_sql: Словарь с SQL запросами для вставки в dimension таблицы.
    :param insert_fact_sql: Словарь с SQL запросами для вставки в факт таблицы.
    :param mark_processed_sql: SQL запрос для пометки событий как обработанных.
    :return: Кортеж (кол-во успешно обработанных событий, список event_id с ошибками).
    """
    rows, columns = fetch_new_staging_events(pg)
    if not rows:
        logging.info("Нет новых событий для обработки.")
        return 0, []

    total_success = 0
    total_errors: List[str] = []
    for i in range(0, len(rows), BATCH_SIZE_DDS):
        batch = rows[i : i + BATCH_SIZE_DDS]
        success, errors = process_batch(batch, columns, pg, insert_dim_sql, insert_fact_sql, mark_processed_sql)
        total_success += success
        total_errors.extend(errors)
        logging.info(f"Итоги обработки батча: успешно вставлено {success} событий, ошибок: {len(errors)}")

    return total_success, total_errors


def load_staging_to_dds(batch_size: int = 50) -> None:
    """
    Загружает события из STAGING слоя в DDS слой:
    - Вставляет уникальные DIM записи
    - Получает surrogate keys
    - Валидирует события через Pydantic
    - Записывает факты в FACT таблицы
    - Отмечает события как обработанные в staging.events

    :param batch_size: Размер батча для обработки.
    :return: None
    """
    start_time = time.time()
    pg = PostgresHook(postgres_conn_id='Postgres')

    notify_telegram("DAG load_dds_from_staging запущен")
    logging.info("Начата загрузка событий из staging.events в DDS")

    insert_dim_sql = get_insert_dim_sql()
    insert_fact_sql = get_insert_fact_sql()
    logging.info(f"Ключи insert_sql перед вставкой: {list(insert_fact_sql.keys())}")
    mark_processed_sql = read_sql_file(SQL_MARK_PROCESSED_STAGING)

    try:
        success_count, error_ids = load_staging_events_batch(
            pg,
            insert_dim_sql,
            insert_fact_sql,
            mark_processed_sql
        )

        # Отправка уведомлений
        notify_telegram(f"[DDS] Успешно обработано событий: {success_count}")
        logging.info(f"[DDS] Успешно обработано событий: {success_count}")

        if error_ids:
            notify_telegram(f"[DDS] Ошибки при обработке {len(error_ids)} событий")
            logging.warning(f"[DDS] Ошибки при обработке событий: {', '.join(error_ids[:10])} ...")

    except Exception as e:
        error_message = f"[DDS] Критическая ошибка загрузки: {e}"
        notify_telegram(error_message)
        logging.error(error_message, exc_info=True)
        raise
    finally:
        elapsed = round(time.time() - start_time, 2)
        notify_telegram(f"[v] DAG load_dds_from_staging завершён. Время выполнения: {elapsed} сек")
        logging.info(f"[DDS] DAG завершён за {elapsed} секунд")


with DAG(
    dag_id='load_dds_from_staging',
    default_args=default_args,
    description='Загрузка событий из staging.events в DDS слой PostgreSQL с валидацией и логированием',
    start_date=datetime(2025, 7, 1),
    schedule_interval=None,
    catchup=False,
    tags=['dds'],
) as dag:
    """
    DAG загружает события из STAGING слоя в DDS слой:
    - Инициализирует схемы и таблицы DDS
    - Загружает батчи событий из staging.events
    - Валидирует данные через Pydantic
    - Записывает в DIM и FACT таблицы
    - Логирует и отправляет уведомления в Telegram
    - После успешной обработки триггерит DAG 'load_clickhouse_from_dds' для переноса данных в marts слой
    """
    init_dds_layer_task = PythonOperator(
        task_id='init_dds_layer',
        python_callable=init_dds_layer_wrapper,
    )

    load_staging_to_dds_task = PythonOperator(
        task_id='load_staging_to_dds',
        python_callable=load_staging_to_dds,
    )

    trigger_clickhouse_dag = TriggerDagRunOperator(
    task_id='trigger_clickhouse_dag',
    trigger_dag_id='load_clickhouse_from_dds',
    wait_for_completion=False,              
    )

    init_dds_layer_task >> load_staging_to_dds_task >> trigger_clickhouse_dag
