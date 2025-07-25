import os
import json
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook

from utils.validation import Event
from utils.telegram_logger import notify_telegram


# Пути к SQL-скриптам
SQL_CREATE_TABLE = os.path.join(os.path.dirname(__file__), '..', 'sql', 'raw', 'create_raw_events.sql')
SQL_INSERT = os.path.join(os.path.dirname(__file__), '..', 'sql', 'raw', 'insert_raw_events.sql')

# Путь к файлу, где будем хранить имена уже обработанных файлов
PROCESSED_FILES_LOG = '/opt/airflow/processed_files.log'

# Параметры DAG — базовые настройки поведения задач
default_args = {
    'owner': 'ilona',                      
    'depends_on_past': False,              
    'retries': 2,                          
    'retry_delay': timedelta(minutes=5),  
}


def read_sql_file(path):
    """
    Читает SQL-скрипт из указанного файла.

    :param path: путь к SQL-файлу
    :return: содержимое SQL-скрипта как строка
    """
    with open(path, 'r') as f:
        return f.read()


def get_processed_files():
    """
    Получает множество имен уже обработанных файлов из лог-файла.

    :return: set строк с именами обработанных файлов
    """
    if not os.path.exists(PROCESSED_FILES_LOG):
        return set()
    with open(PROCESSED_FILES_LOG, 'r') as f:
        return set(line.strip() for line in f.readlines())


def mark_file_as_processed(filename):
    """
    Добавляет имя файла в лог уже обработанных.

    :param filename: имя обработанного файла
    """
    with open(PROCESSED_FILES_LOG, 'a') as f:
        f.write(f"{filename}\n")


def create_table():
    """
    Проверяет и при необходимости создаёт таблицу raw.events.

    Использует SQL-скрипт из sql/raw/create_raw_events.sql.
    """
    pg_hook = PostgresHook(postgres_conn_id='Postgres')
    sql = read_sql_file(SQL_CREATE_TABLE)
    pg_hook.run(sql)
    logging.info("Таблица raw.events проверена/создана.")


def load_from_minio_to_postgres():
    """
    Загружает новые события из MinIO в PostgreSQL:

    - Получает список новых файлов
    - Проводит базовую валидацию структуры
    - Выполняет вставку в таблицу raw.events
    - Отправляет сводные логи в Telegram (старт, количество, успех/ошибки, завершение)
    - Запоминает уже обработанные файлы
    """
    s3 = S3Hook(aws_conn_id='MinIO')
    pg = PostgresHook(postgres_conn_id='Postgres')

    notify_telegram("DAG load_raw_from_minio запущен")

    # Прочитать уже обработанные файлы
    processed_files = get_processed_files()

    # Получить список всех объектов из bucket
    bucket_name = os.getenv('MINIO_BUCKET_NAME', 'events')  # дефолт на всякий случай
    files = s3.list_keys(bucket_name=bucket_name)

    if not files:
        msg = "Нет новых файлов в MinIO."
        logging.info(msg)
        notify_telegram(msg)
        notify_telegram("[v] DAG load_raw_from_minio успешно завершён")
        return

    new_files = [f for f in files if f not in processed_files]
    if not new_files:
        msg = "Все файлы уже обработаны."
        logging.info(msg)
        notify_telegram(msg)
        notify_telegram("[v] DAG load_raw_from_minio успешно завершён")
        return

    insert_sql = read_sql_file(SQL_INSERT)

    success_count = 0
    errors = []

    for file_key in sorted(new_files):
        try:
            file_obj = s3.read_key(key=file_key, bucket_name=bucket_name)
            data = json.loads(file_obj)

            # Валидация данных
            validated = Event(**data)

            # Подготовка строки для вставки
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
                'campaign': validated.marketing.campaign,
                'promocode': validated.marketing.promocode,
                'user_campaign_id': validated.marketing.user_campaign_id,
                'raw_payload': json.dumps(data),
            }

            pg.run(insert_sql, parameters=row)
            mark_file_as_processed(file_key)
            success_count += 1
        except Exception as e:
            logging.error(f"Ошибка при обработке файла {file_key}: {e}")
            errors.append(f"{file_key}: {e}")

    # Итоговые уведомления
    notify_telegram(f"Обработано файлов: {success_count} из {len(new_files)}")
    if errors:
        error_msg = "\n".join(errors)
        notify_telegram(f"[x] Ошибки при обработке файлов:\n{error_msg}")

    notify_telegram("[v] DAG load_raw_from_minio успешно завершён")


# Определение DAG-а и последовательности задач
with DAG(
    dag_id='load_raw_from_minio',
    default_args=default_args,
    description='Загрузка событий из MinIO в raw слой PostgreSQL с валидацией и логированием',
    start_date=datetime(2025, 7, 1),
    schedule_interval='@hourly',
    catchup=False,
    tags=['raw', 'minio', 'validation'],
) as dag:

    create_table_task = PythonOperator(
        task_id='create_raw_events_table',
        python_callable=create_table,
    )

    load_data_task = PythonOperator(
        task_id='load_and_validate_data',
        python_callable=load_from_minio_to_postgres,
    )

    create_table_task >> load_data_task
