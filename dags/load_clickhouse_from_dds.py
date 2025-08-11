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

def load_clickhouse_marts_wrapper() -> None:
    """
    Обёртка для запуска функции загрузки marts в ClickHouse с логированием и уведомлениями.

    Выполняет:
    - логирование начала и конца работы DAG,
    - отправку уведомлений в Telegram о старте и завершении,
    - перехват и логирование исключений с уведомлением в Telegram,
    - проброс исключения для корректной обработки Airflow.

    :return: None
    :raises Exception: Пробрасывает исключения из функции load_clickhouse_marts для обработки Airflow.
    """
    try:
        logging.info("DAG load_clickhouse_from_dds запущен")
        notify_telegram("DAG load_clickhouse_from_dds запущен")

        load_clickhouse_marts()

        logging.info("DAG load_clickhouse_from_dds завершён")
        notify_telegram("[v] DAG load_clickhouse_from_dds завершён")

    except Exception as e:
        logging.error(f"[x] Ошибка в DAG load_clickhouse_from_dds: {e}")
        notify_telegram(f"[x] Ошибка в DAG load_clickhouse_from_dds: {e}")
        raise


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
        python_callable=load_clickhouse_marts_wrapper,
    )

    load_ch_task
