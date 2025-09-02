import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.python import PythonOperator
from airflow.utils import timezone

from utils.telegram_logger import notify_telegram
from utils.loading.marts.marts_loader import create_ch_tables, load_daily_summary_to_ch, load_product_stats_to_ch, load_user_behavior_to_ch

# Аргументы DAG по умолчанию
default_args = {
    "owner": "ilona",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

def log_dag_start(**context) -> None:
    """
    Логирует старт выполнения DAG без лишней информации.

    :param context: Контекст Airflow.
    :return: None
    :raises Exception: Если не удаётся отправить сообщение в Telegram.
    """
    dag_id = context["dag"].dag_id
    message = f"DAG {dag_id} запущен"
    logging.info(message)
    try:
        notify_telegram(message)
    except Exception as e:
        logging.error(f"Ошибка при отправке уведомления о старте DAG {dag_id}: {e}")


def log_dag_end(**context) -> None:
    """
    Отправляет уведомление о завершении DAG с указанием времени выполнения.
    
    Используется как PythonOperator или callback для DAG. 
    Корректно учитывает таймзону start_date из dag_run.
    
    :param context: Контекст Airflow с информацией о dag_run.
    :return: None
    """
    dag_run = context.get('dag_run')
    if not dag_run:
        logging.error("dag_run не найден в контексте!")
        return

    dag_id = dag_run.dag_id
    start_time = dag_run.start_date
    end_time = datetime.now(tz=timezone.utc).astimezone(start_time.tzinfo)
    duration = (end_time - start_time).total_seconds()

    message = f"[v] DAG {dag_id} завершён. Время выполнения: {duration:.2f} сек"
    logging.info(message)
    try:
        notify_telegram(message)
    except Exception as e:
        logging.error(f"Ошибка при отправке уведомления: {e}")
        

with DAG(
    dag_id="load_clickhouse_from_dds",
    default_args=default_args,
    description="Загрузка агрегированных витрин marts из DDS (Postgres) в ClickHouse с использованием ReplacingMergeTree",
    start_date=datetime(2025, 8, 1),
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    tags=["clickhouse", "marts"],
) as dag:
    """
    DAG для загрузки данных marts из DDS (Postgres) в ClickHouse.

    Шаги:
    - Логирование старта DAG
    - Создание таблиц витрин (если ещё не созданы)
    - Загрузка ежедневной сводки
    - Загрузка статистики товаров
    - Загрузка поведения пользователей
    - Использование версии данных (timestamp) для замены старых записей ReplacingMergeTree
    """

    log_start = PythonOperator(
        task_id="log_dag_start",
        python_callable=log_dag_start,
    )

    create_tables = PythonOperator(
        task_id="create_ch_tables",
        python_callable=create_ch_tables,
    )

    load_daily = PythonOperator(
        task_id="load_daily_summary_to_ch",
        python_callable=load_daily_summary_to_ch,
    )

    load_products = PythonOperator(
        task_id="load_product_stats_to_ch",
        python_callable=load_product_stats_to_ch,
    )

    load_users = PythonOperator(
        task_id="load_user_behavior_to_ch",
        python_callable=load_user_behavior_to_ch,
    )

    notify_end = PythonOperator(
    task_id='notify_dag_end',
    python_callable=log_dag_end,
    trigger_rule=TriggerRule.ALL_DONE,
    )

    log_start >> create_tables >> load_daily >> load_products >> load_users  >> notify_end