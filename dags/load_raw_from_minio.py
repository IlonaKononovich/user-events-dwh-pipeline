import os
from datetime import datetime, timedelta
import logging
from typing import List, Dict, Any

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from utils.telegram_logger import notify_telegram
from utils.sql_db.schema_and_tables_init import init_raw_layer, mark_file_as_processed
from utils.loading.raw.raw_loader import get_new_files
from utils.constants import RAW_EVENT_COLUMNS, RAW_EVENT_INVALID_COLUMNS
from utils.loading.raw.raw_loader import parse_file, insert_batch


# Аргументы DAG по умолчанию
default_args = {
    'owner': 'ilona',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

def init_raw_layer_wrapper() -> None:
    """
    Обёртка для инициализации RAW-слоя (схема + таблицы)
    :return: None

    """
    pg = PostgresHook(postgres_conn_id='Postgres')
    init_raw_layer(pg)


def load_from_minio_to_postgres(batch_size: int = 10) -> None:
    """
    Загружает события из MinIO в raw.events батчами с защитой от сбоев.

    :param batch_size: Размер батча для вставки в БД.
    :return: None
    """
    s3 = S3Hook(aws_conn_id='MinIO')
    pg = PostgresHook(postgres_conn_id='Postgres')

    notify_telegram("DAG load_raw_from_minio запущен")
    logging.info("Загрузка новых событий из MinIO в raw.events начата")

    bucket = os.getenv('MINIO_BUCKET', 'events')
    new_files = get_new_files(s3, pg, bucket)

    if not new_files:
        notify_telegram("Нет новых файлов для обработки.")
        notify_telegram("[v] DAG load_raw_from_minio успешно завершён")
        logging.info("Новых файлов для обработки не найдено")
        return

    batch_rows: List[Dict[str, Any]] = []
    error_rows: List[Dict[str, Any]] = []
    success_count = 0

    # Используем множества, чтобы избежать дублирования файлов
    success_files: set[str] = set()
    error_files: set[str] = set()

    for file_key in sorted(new_files):
        row, error_row = parse_file(file_key, s3, bucket)

        if row:
            batch_rows.append(row)
            success_files.add(file_key)
            success_count += 1
        if error_row:
            error_rows.append(error_row)
            error_files.add(file_key)

        # Вставка батчей успешных событий
        if len(batch_rows) >= batch_size:
            if insert_batch(pg, "raw.events", batch_rows, RAW_EVENT_COLUMNS, batch_type="events"):
                for f in success_files:
                    mark_file_as_processed(pg, f)
                success_files.clear()
            batch_rows.clear()

        # Вставка батчей с ошибками
        if len(error_rows) >= batch_size:
            if insert_batch(pg, "raw.events_invalid", error_rows, RAW_EVENT_INVALID_COLUMNS, batch_type="invalid"):
                for f in error_files:
                    mark_file_as_processed(pg, f)
                error_files.clear()
            error_rows.clear()

    # Вставляем остаток
    if batch_rows:
        if insert_batch(pg, "raw.events", batch_rows, RAW_EVENT_COLUMNS, batch_type="events"):
            for f in success_files:
                mark_file_as_processed(pg, f)
    if error_rows:
        if insert_batch(pg, "raw.events_invalid", error_rows, RAW_EVENT_INVALID_COLUMNS, batch_type="invalid"):
            for f in error_files:
                mark_file_as_processed(pg, f)

    notify_telegram(f"Обработано файлов: {success_count} из {len(new_files)}")
    notify_telegram("[v] DAG load_raw_from_minio успешно завершён")
    logging.info(f"Загрузка завершена. Успешно обработано файлов: {success_count}")



with DAG(
    dag_id='load_raw_from_minio',
    default_args=default_args,
    description='Загрузка событий из MinIO в raw слой PostgreSQL с валидацией и логированием',
    start_date=datetime(2025, 7, 1),
    schedule_interval="* * * * *",
    catchup=False,
    max_active_runs=1,
    tags=['raw'],
) as dag:
    """
    DAG загружает JSON-события из MinIO в PostgreSQL:
    - Проверяет наличие новых файлов
    - Валидирует через Pydantic
    - Записывает в таблицу raw.events
    - Логирует обработанные файлы
    - После успешной обработки триггерит DAG 'load_staging_from_raw' для переноса данных в STAGING слой
    """
    init_raw_layer_task = PythonOperator(
        task_id='init_raw_layer',
        python_callable=init_raw_layer_wrapper,
    )

    load_and_validate_data_task = PythonOperator(
        task_id='load_and_validate_data',
        python_callable=load_from_minio_to_postgres,
    )

    trigger_staging_dag = TriggerDagRunOperator(
    task_id='trigger_staging_dag',
    trigger_dag_id='load_staging_from_raw',
    wait_for_completion=False,              
    )

    init_raw_layer_task >> load_and_validate_data_task >> trigger_staging_dag
