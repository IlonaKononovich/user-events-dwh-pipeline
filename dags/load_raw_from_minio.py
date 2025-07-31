import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook

from utils.telegram_logger import notify_telegram
from utils.sql_db.sql_utils import read_sql_file
from utils.sql_db.schema_and_tables_init import init_raw_layer
from utils.loading.raw_loader import get_new_files, process_file
from utils.sql_db.sql_paths import SQL_INSERT_EVENT


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


def load_from_minio_to_postgres() -> None:
    """
    Основная функция загрузки новых событий из MinIO в raw.events:
    - Получает новые файлы
    - Обрабатывает каждый файл через функцию process_file
    - Логирует успешные и ошибочные обработки
    - Отправляет уведомления в Telegram

    :raises Exception: при ошибках чтения, десериализации или валидации
    """
    s3 = S3Hook(aws_conn_id='MinIO')
    pg = PostgresHook(postgres_conn_id='Postgres')

    notify_telegram("DAG load_raw_from_minio запущен")

    bucket = os.getenv('MINIO_BUCKET', 'events')
    new_files = get_new_files(s3, pg, bucket)

    if not new_files:
        notify_telegram("Нет новых файлов для обработки.")
        notify_telegram("[v] DAG load_raw_from_minio успешно завершён")
        return

    insert_sql = read_sql_file(SQL_INSERT_EVENT)
    success_count = 0
    errors = []

    for file_key in sorted(new_files):
        success, error_msg = process_file(file_key, s3, pg, insert_sql, bucket)
        if success:
            success_count += 1
        else:
            errors.append(error_msg)

    notify_telegram(f"Обработано файлов: {success_count} из {len(new_files)}")
    if errors:
        notify_telegram(f"[x] Ошибки при обработке:\n" + "\n".join(errors))

    notify_telegram("[v] DAG load_raw_from_minio успешно завершён")


with DAG(
    dag_id='load_raw_from_minio',
    default_args=default_args,
    description='Загрузка событий из MinIO в raw слой PostgreSQL с валидацией и логированием',
    start_date=datetime(2025, 7, 1),
    schedule_interval=None,
    catchup=False,
    tags=['raw'],
) as dag:
    """
    DAG загружает JSON-события из MinIO в PostgreSQL:
    - Проверяет наличие новых файлов
    - Валидирует через Pydantic
    - Записывает в таблицу raw.events
    - Логирует обработанные файлы
    """
    init_raw_layer_task = PythonOperator(
        task_id='init_raw_layer',
        python_callable=init_raw_layer_wrapper,
    )

    load_and_validate_data_task = PythonOperator(
        task_id='load_and_validate_data',
        python_callable=load_from_minio_to_postgres,
    )

    init_raw_layer_task >> load_and_validate_data_task
