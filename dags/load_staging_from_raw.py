import time
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from utils.telegram_logger import notify_telegram
from utils.sql_db.schema_and_tables_init import init_staging_layer
from utils.sql_db.sql_paths import SQL_INSERT_STAGING_EVENT
from utils.sql_db.sql_utils import read_sql_file


# Аргументы DAG по умолчанию
default_args = {
    'owner': 'ilona',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}


def init_staging_layer_wrapper() -> None:
    """
    Обёртка для инициализации STAGING-слоя (схема + таблицы)
    :return: None
    """
    pg = PostgresHook(postgres_conn_id='Postgres')
    init_staging_layer(pg)


def load_staging_events(batch_limit: int = 5000) -> None:
    """
    Загружает данные из RAW слоя в STAGING слой:
    - Читает новые события из raw.events
    - Переносит их в staging.events
    - Логирует и отправляет уведомления в Telegram

    :param batch_limit: Максимальное количество строк для загрузки за один запуск.
    :return: None
    """
    start_time = time.time()
    pg = PostgresHook(postgres_conn_id='Postgres')

    notify_telegram("DAG load_staging_from_raw запущен")
    logging.info("Начата загрузка данных из raw.events в staging.events")

    sql_template = read_sql_file(SQL_INSERT_STAGING_EVENT)
    sql = sql_template.format(batch_limit=batch_limit)

    try:
        conn = pg.get_conn()
        with conn.cursor() as cur:
            cur.execute(sql)
            inserted = cur.rowcount
        conn.commit()

        # Логируем результат
        if inserted == 0:
            message = "[STAGING] Новых событий для загрузки не найдено"
            logging.info(message)
            notify_telegram(message)
        else:
            message = f"[STAGING] Загружено {inserted} строк из RAW в STAGING"
            logging.info(message)
            notify_telegram(message)

    except Exception as e:
        error_message = f"[STAGING] Ошибка загрузки в STAGING: {e}"
        logging.error(error_message, exc_info=True)
        notify_telegram(error_message)
        raise
    finally:
        elapsed = round(time.time() - start_time, 2)
        notify_telegram(f"[v] DAG load_staging_from_raw завершён. Время выполнения: {elapsed} сек")
        logging.info(f"[STAGING] DAG завершён за {elapsed} секунд")


with DAG(
    dag_id='load_staging_from_raw',
    default_args=default_args,
    description='Загрузка событий из raw слоя в staging слой PostgreSQL с логированием и уведомлениями',
    start_date=datetime(2025, 7, 1),
    schedule_interval=None,
    catchup=False,
    tags=['staging'],
) as dag:
    """
    DAG загружает события из RAW слоя в STAGING слой:
    - Инициализирует схему и таблицы STAGING
    - Переносит новые события батчами
    - Логирует процесс и отправляет уведомления в Telegram
    """
    init_staging_layer_task = PythonOperator(
        task_id='init_staging_layer',
        python_callable=init_staging_layer_wrapper,
    )

    load_staging_task = PythonOperator(
        task_id='load_staging_events',
        python_callable=load_staging_events,
    )

    init_staging_layer_task >> load_staging_task
