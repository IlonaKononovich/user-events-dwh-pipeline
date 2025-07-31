import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from utils.telegram_logger import notify_telegram
from utils.sql_db.sql_utils import read_sql_file, init_dds_layer
from utils.loading.dds_loader import fetch_new_raw_events, process_event, get_insert_dim_sql, get_insert_fact_sql
from utils.validation.sql.sql_paths import BASE_DIR, BASE_SQL_RAW_INSERT, BASE_SQL_DDS_INSERT, SQL_MARK_EVENT_PROCESSED



default_args = {
    'owner': 'ilona',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

def init_dds_layer_wrapper() -> None:
    """
    Обёртка для инициализации DDS-слоя (схема + таблицы)
    и таблицы raw.processed_events

    :return: None
    """
    pg = PostgresHook(postgres_conn_id='Postgres')
    init_dds_layer(pg)


def load_raw_to_dds() -> None:
    """
    Загружает необработанные события из raw слоя в DDS слой, валидирует и вставляет данные в DIM и FACT таблицы.
    Отмечает события как обработанные и отправляет уведомления в Telegram.

    Последовательность действий:
    1. Подключается к Postgres через PostgresHook.
    2. Получает необработанные события из raw.events.
    3. Если событий нет — уведомляет и завершает выполнение.
    4. Загружает SQL для вставки в DIM и FACT таблицы.
    5. Обрабатывает каждое событие через функцию process_event.
    6. Подсчитывает успешные обработки и собирает ошибки.
    7. Отправляет итоговые уведомления в Telegram.

    :return: None — функция выполняет загрузку и логирование, не возвращает значения.
    :raises Exception: Пробрасывает исключения, если возникает ошибка соединения с БД,
                      чтения файлов SQL или выполнения SQL запросов в процессе обработки.
    """
    pg = PostgresHook(postgres_conn_id='Postgres')
    notify_telegram("DAG load_dds_from_raw запущен")

    rows, columns = fetch_new_raw_events(pg)

    if not rows:
        notify_telegram("Новых событий для обработки нет")
        return

    insert_dim_sql = get_insert_dim_sql()
    insert_fact_sql = get_insert_fact_sql()
    mark_processed_sql = read_sql_file(SQL_MARK_EVENT_PROCESSED)

    success_count = 0
    errors = []

    for row in rows:
        if process_event(row, columns, pg, insert_dim_sql, insert_fact_sql, mark_processed_sql):
            success_count += 1
        else:
            errors.append(str(row[0]))

    notify_telegram(f"Обработано событий: {success_count} из {len(rows)}")
    if errors:
        notify_telegram(f"[x] Ошибки при обработке событий: {', '.join(errors)}")
    notify_telegram("[v] DAG load_dds_from_raw успешно завершён")




with DAG(
    dag_id='load_dds_from_raw',
    default_args=default_args,
    description='Загрузка данных из raw.events в DDS слой с валидацией и логированием.',
    start_date=datetime(2025, 7, 1),
    schedule_interval=None,
    catchup=False,
    tags=['dds'],
) as dag:
    """
    DAG загружает и валидирует необработанные события из слоя raw в слой DDS (звёздная схема):
    - Проверяет наличие новых событий в raw.events
    - Валидирует данные через Pydantic-модели
    - Создаёт таблицы в DDS, если они не созданы
    - Загружает данные в DIM и FACT таблицы
    - Отмечает события как обработанные в raw.processed_events
    - Отправляет уведомления в Telegram
    """
    init_dds_layer_task = PythonOperator(
        task_id='init_dds_layer',
        python_callable=init_dds_layer_wrapper,
    )

    load_raw_events_to_dds_task = PythonOperator(
        task_id='load_raw_events_to_dds',
        python_callable=load_raw_to_dds,
    )

    init_dds_layer_task >> load_raw_events_to_dds_task
