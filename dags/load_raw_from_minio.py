import os
import json
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook

from utils.raw_validation import Event
from utils.telegram_logger import notify_telegram

# Пути к SQL-скриптам
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))

BASE_SQL_RAW_CREATE = os.path.join(BASE_DIR, 'sql', 'raw', 'create')
BASE_SQL_RAW_INSERT = os.path.join(BASE_DIR, 'sql', 'raw', 'insert')

SQL_CREATE_EVENTS = os.path.join(BASE_SQL_RAW_CREATE, 'create_raw_events.sql')
SQL_CREATE_PROCESSED = os.path.join(BASE_SQL_RAW_CREATE, 'create_processed_files.sql')

SQL_INSERT_EVENT = os.path.join(BASE_SQL_RAW_INSERT, 'insert_raw_events.sql')
SQL_MARK_PROCESSED = os.path.join(BASE_SQL_RAW_INSERT, 'mark_file_processed.sql')

# Аргументы DAG по умолчанию
default_args = {
    'owner': 'ilona',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}


def read_sql_file(path: str) -> str:
    """
    Читает SQL-скрипт из файла.

    :param path: Путь до .sql-файла
    :return: Строка с SQL-кодом
    """
    with open(path, 'r') as f:
        return f.read()


def get_processed_files(pg_hook: PostgresHook) -> set:
    """
    Получает множество имён уже обработанных файлов из таблицы raw.processed_files.

    :param pg_hook: Инстанс PostgresHook с активным соединением
    :return: Множество имён файлов
    """
    sql = "SELECT filename FROM raw.processed_files"
    return {r[0] for r in pg_hook.get_records(sql)}


def mark_file_as_processed(pg_hook: PostgresHook, filename: str) -> None:
    """
    Добавляет имя обработанного файла в таблицу raw.processed_files.

    :param pg_hook: Инстанс PostgresHook
    :param filename: Имя файла, который был успешно обработан
    """
    sql = read_sql_file(SQL_MARK_PROCESSED)
    pg_hook.run(sql, parameters=(filename,))


def create_raw_events_table() -> None:
    """
    Создаёт таблицу raw.events, если она ещё не существует.
    """
    pg = PostgresHook(postgres_conn_id='Postgres')
    pg.run(read_sql_file(SQL_CREATE_EVENTS))
    logging.info("Таблица raw.events проверена/создана.")


def create_processed_files_table() -> None:
    """
    Создаёт таблицу raw.processed_files, если она ещё не существует.
    """
    pg = PostgresHook(postgres_conn_id='Postgres')
    pg.run(read_sql_file(SQL_CREATE_PROCESSED))
    logging.info("Таблица raw.processed_files проверена/создана.")


def load_from_minio_to_postgres() -> None:
    """
    Загружает новые события из файлов в MinIO в таблицу raw.events:
    - Проверяет наличие необработанных файлов
    - Валидирует каждый файл через Pydantic
    - Записывает в PostgreSQL
    - Фиксирует имя файла как обработанное
    - Отправляет уведомления в Telegram при успехе и ошибках

    :raises Exception: при ошибке чтения, десериализации или валидации
    """
    s3 = S3Hook(aws_conn_id='MinIO')
    pg = PostgresHook(postgres_conn_id='Postgres')

    notify_telegram("DAG load_raw_from_minio запущен")
    create_processed_files_table()

    processed_files = get_processed_files(pg)
    bucket = os.getenv('MINIO_BUCKET_NAME', 'events')
    files = s3.list_keys(bucket_name=bucket) or []
    new_files = [f for f in files if f not in processed_files]

    if not new_files:
        notify_telegram("Нет новых файлов для обработки.")
        notify_telegram("[v] DAG load_raw_from_minio успешно завершён")
        return

    insert_sql = read_sql_file(SQL_INSERT_EVENT)
    success_count = 0
    errors = []

    for file_key in sorted(new_files):
        try:
            content = s3.read_key(bucket_name=bucket, key=file_key)
            if isinstance(content, bytes):
                content = content.decode("utf-8")
            data = json.loads(content)
            validated = Event(**data)

            row = {
                'event_id': validated.event_id,
                'event_time': validated.event_time,
                'user_id': validated.user.user_id,
                'email': validated.user.email,
                'referral_code': validated.user.referral_code,
                'user_name': validated.user.profile.name,
                'birth_date': validated.user.profile.birth_date,
                'profile_created_at': validated.user.profile.created_at,
                'product_id': validated.product.product_id,
                'product_name': validated.product.name,
                'category': validated.product.category,
                'supplier': validated.product.supplier,
                'price': validated.product.price,
                'session_id': validated.session.session_id,
                'session_start_time': validated.session.start_time,
                'session_end_time': validated.session.end_time,
                'device_type': validated.session.device.type,
                'device_os': validated.session.device.os,
                'location_country': validated.session.device.location.country,
                'location_city': validated.session.device.location.city,
                'order_id': validated.order.order_id,
                'payment_id': validated.order.payment_id,
                'order_items': validated.order.order_items,
                'total_amount': validated.order.total_amount,
                'order_status': validated.order.status,
                'campaign': validated.marketing.campaign or '',
                'promocode': validated.marketing.promocode or '',
                'user_campaign_id': validated.marketing.user_campaign_id,
                'raw_payload': json.dumps(data),
            }

            pg.run(insert_sql, parameters=row)
            mark_file_as_processed(pg, file_key)
            success_count += 1

        except Exception as e:
            logging.exception(f"Ошибка при обработке {file_key}")
            errors.append(f"{file_key}: {e}")

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
    tags=['raw', 'minio', 'validation'],
) as dag:
    """
    DAG загружает JSON-события из MinIO в PostgreSQL:
    - Проверяет наличие новых файлов
    - Валидирует через Pydantic
    - Записывает в таблицу raw.events
    - Логирует обработанные файлы
    """
    create_raw_events_table_task = PythonOperator(
        task_id='create_raw_events_table',
        python_callable=create_raw_events_table,
    )

    load_and_validate_data_task = PythonOperator(
        task_id='load_and_validate_data',
        python_callable=load_from_minio_to_postgres,
    )

    create_raw_events_table_task >> load_and_validate_data_task
