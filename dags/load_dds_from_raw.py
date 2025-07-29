import os
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from utils.dds_validation import FactOrder
from utils.telegram_logger import notify_telegram

# Пути к SQL-скриптам
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))

BASE_SQL_RAW_CREATE = os.path.join(BASE_DIR, 'sql', 'raw', 'create')
BASE_SQL_RAW_INSERT = os.path.join(BASE_DIR, 'sql', 'raw', 'insert')
BASE_SQL_DDS_CREATE = os.path.join(BASE_DIR, 'sql', 'dds', 'create')
BASE_SQL_DDS_INSERT = os.path.join(BASE_DIR, 'sql', 'dds', 'insert')

SQL_CREATE_PROCESSED_EVENTS = os.path.join(BASE_SQL_RAW_CREATE, 'create_processed_events.sql')
SQL_MARK_EVENT_PROCESSED = os.path.join(BASE_SQL_RAW_INSERT, 'mark_event_processed.sql')

default_args = {
    'owner': 'ilona',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}


def read_sql_file(path: str) -> str:
    """
    Читает SQL-скрипт из файла.

    :param path: Путь к .sql файлу
    :return: SQL-код в виде строки
    """
    with open(path, 'r') as f:
        return f.read()


def create_processed_events_table() -> None:
    """
    Создаёт таблицу raw.processed_events, если её нет.

    :return: None
    """
    pg = PostgresHook(postgres_conn_id='Postgres')
    pg.run(read_sql_file(SQL_CREATE_PROCESSED_EVENTS), autocommit=True)
    logging.info("Таблица raw.processed_events проверена/создана.")

def create_dds_schema() -> None:
    """
    Создаёт схему dds, если её нет.

    :return: None
    """
    pg = PostgresHook(postgres_conn_id='Postgres')
    schema_path = os.path.join(BASE_SQL_DDS_CREATE, 'create_dds_schema.sql') 
    sql = read_sql_file(schema_path)
    pg.run(sql, autocommit=True)
    logging.info("Схема dds проверена/создана.")


def create_dds_tables() -> None:
    """
    Создаёт схему и все таблицы DDS.

    :return: None
    """
    create_dds_schema()

    pg = PostgresHook(postgres_conn_id='Postgres')
    create_scripts = sorted(f for f in os.listdir(BASE_SQL_DDS_CREATE) if f != 'create_dds_schema.sql')

    for script in create_scripts:
        path = os.path.join(BASE_SQL_DDS_CREATE, script)
        sql = read_sql_file(path)
        pg.run(sql, autocommit=True)
        logging.info(f"Таблица по скрипту {script} проверена/создана.")

    notify_telegram("[v] Все таблицы DDS созданы или уже существуют.")



def load_dim_data(event_dict: dict, pg: PostgresHook, insert_dim_sql: dict) -> None:
    """
    Вставляет данные в DIM таблицы.

    :param event_dict: Словарь с данными события для вставки
    :param pg: Инстанс PostgresHook для выполнения SQL
    :param insert_dim_sql: Словарь с SQL для вставки в DIM таблицы
    :return: None
    """
    for dim, sql in insert_dim_sql.items():
        pg.run(sql, parameters=event_dict)


def load_fact_data(event_dict: dict, pg: PostgresHook, insert_fact_sql: str) -> None:
    """
    Вставляет данные в FACT таблицу.

    :param event_dict: Словарь с данными события для вставки
    :param pg: Инстанс PostgresHook для выполнения SQL
    :param insert_fact_sql: SQL для вставки в FACT таблицу
    :return: None
    """
    pg.run(insert_fact_sql, parameters=event_dict)


def load_raw_to_dds() -> None:
    """
    Загружает новые строки из raw.events в DDS:
    - выбирает необработанные события,
    - валидирует через Pydantic-модель,
    - вставляет в dim и fact таблицы,
    - отмечает событие как обработанное.

    :return: None
    """
    pg = PostgresHook(postgres_conn_id='Postgres')
    notify_telegram("DAG load_dds_from_raw запущен")

    sql_new_events = """
    SELECT * FROM raw.events e
    WHERE NOT EXISTS (
        SELECT 1 FROM raw.processed_events p WHERE p.event_id = e.event_id
    )
    ORDER BY event_time;
    """

    # Открываем курсор и выполняем запрос
    conn = pg.get_conn()
    cursor = conn.cursor()
    cursor.execute(sql_new_events)
    rows = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]

    if not rows:
        notify_telegram("Новых событий для обработки нет")
        return

    insert_dim_sql = {
        'date': read_sql_file(os.path.join(BASE_SQL_DDS_INSERT, 'insert_dim_date.sql')),
        'device': read_sql_file(os.path.join(BASE_SQL_DDS_INSERT, 'insert_dim_device.sql')),
        'location': read_sql_file(os.path.join(BASE_SQL_DDS_INSERT, 'insert_dim_location.sql')),
        'product': read_sql_file(os.path.join(BASE_SQL_DDS_INSERT, 'insert_dim_product.sql')),
        'session': read_sql_file(os.path.join(BASE_SQL_DDS_INSERT, 'insert_dim_session.sql')),
        'user': read_sql_file(os.path.join(BASE_SQL_DDS_INSERT, 'insert_dim_user.sql')),
    }
    insert_fact_order_sql = read_sql_file(os.path.join(BASE_SQL_DDS_INSERT, 'insert_fact_order.sql'))
    mark_processed_sql = read_sql_file(SQL_MARK_EVENT_PROCESSED)

    success_count, errors = 0, []

    for row in rows:
        try:
            event_dict = dict(zip(columns, row))
            validated = FactOrder(**event_dict)

            load_dim_data(event_dict, pg, insert_dim_sql)
            load_fact_data(event_dict, pg, insert_fact_order_sql)

            pg.run(mark_processed_sql, parameters=(validated.event_id,))
            success_count += 1

        except Exception as e:
            logging.exception(f"Ошибка при обработке события {row[0]}")
            errors.append(f"{row[0]}: {e}")

    notify_telegram(f"Обработано событий: {success_count} из {len(rows)}")
    if errors:
        notify_telegram(f"[x] Ошибки:\n" + "\n".join(errors))
    notify_telegram("[v] DAG load_dds_from_raw успешно завершён")


with DAG(
    dag_id='load_dds_from_raw',
    default_args=default_args,
    description='Загрузка данных из raw.events в DDS слой с валидацией и логированием.',
    start_date=datetime(2025, 7, 1),
    schedule_interval=None,
    catchup=False,
    tags=['dds', 'raw', 'validation'],
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
    create_processed_events_table_task = PythonOperator(
        task_id='create_processed_events_table',
        python_callable=create_processed_events_table,
    )

    create_dds_tables_task = PythonOperator(
        task_id='create_dds_tables',
        python_callable=create_dds_tables,
    )

    load_raw_events_to_dds_task = PythonOperator(
        task_id='load_raw_events_to_dds',
        python_callable=load_raw_to_dds,
    )

    create_processed_events_table_task >> create_dds_tables_task >> load_raw_events_to_dds_task
