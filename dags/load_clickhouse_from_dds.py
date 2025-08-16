import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from utils.telegram_logger import notify_telegram
from utils.loading.marts.marts_loader import load_clickhouse_marts

# Аргументы DAG по умолчанию
default_args = {
    "owner": "ilona",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="load_clickhouse_from_dds",
    default_args=default_args,
    description="Загрузка агрегированных витрин marts из DDS (Postgres) в ClickHouse с использованием ReplacingMergeTree",
    start_date=datetime(2025, 8, 1),
    schedule_interval=None,
    catchup=False,
    tags=["clickhouse", "marts"],
) as dag:
    """
    DAG для загрузки данных marts в ClickHouse.

    Шаги:
    - Создание таблиц витрин (если ещё не созданы)
    - Загрузка ежедневной сводки, статистики товаров и поведения пользователей из DDS
    - Использование версии данных (timestamp) для замены старых записей ReplacingMergeTree
    """

    load_ch_task = PythonOperator(
        task_id="load_clickhouse_marts",
        python_callable=load_clickhouse_marts,
    )

    load_ch_task
