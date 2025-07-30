import os
import logging
from airflow.providers.postgres.hooks.postgres import PostgresHook
from utils.dds_validation import FactOrder
from utils.db_utils import read_sql_file
from typing import List, Tuple, Dict

# Пути к SQL-скриптам
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
BASE_SQL_RAW_SELECT = os.path.join(BASE_DIR, 'sql', 'raw', 'select')
BASE_SQL_DDS_INSERT = os.path.join(BASE_DIR, 'sql', 'dds', 'insert')
SQL_SELECT_NEW_EVENTS = os.path.join(BASE_SQL_RAW_SELECT, 'select_raw_events.sql')


def load_dim_data(event_dict: Dict[str, object], pg: PostgresHook, insert_dim_sql: Dict[str, str]) -> None:
    """
    Вставляет данные в DIM таблицы.

    :param event_dict: Словарь с данными события для вставки
    :param pg: Инстанс PostgresHook для выполнения SQL
    :param insert_dim_sql: Словарь с SQL для вставки в DIM таблицы
    :return: None
    """
    for dim, sql in insert_dim_sql.items():
        pg.run(sql, parameters=event_dict, autocommit=True)


def load_fact_data(event_dict: Dict[str, object], pg: PostgresHook, insert_fact_sql: str) -> None:
    """
    Вставляет данные в FACT таблицу.

    :param event_dict: Словарь с данными события для вставки
    :param pg: Инстанс PostgresHook для выполнения SQL
    :param insert_fact_sql: SQL для вставки в FACT таблицу
    :return: None
    """
    pg.run(insert_fact_sql, parameters=event_dict, autocommit=True)


def get_insert_dim_sql() -> Dict[str, str]:
    """
    Загружает все SQL-скрипты для вставки данных в DIM таблицы.
    Автоматически ищет файлы вида insert_dim_*.sql в папке sql/dds/insert.

    :return: Словарь {имя_измерения: SQL-код}
    """
    dim_sql = {}
    try:
        for filename in os.listdir(BASE_SQL_DDS_INSERT):
            if filename.startswith('insert_dim_') and filename.endswith('.sql'):
                dim_name = filename.replace('insert_', '').replace('.sql', '')
                path = os.path.join(BASE_SQL_DDS_INSERT, filename)
                dim_sql[dim_name] = read_sql_file(path)
    except Exception as e:
        logging.error(f"Ошибка при загрузке DIM SQL из {BASE_SQL_DDS_INSERT}: {e}")
        raise

    return dim_sql


def fetch_new_raw_events(pg: PostgresHook) -> Tuple[List[tuple], List[str]]:
    """
    Выбирает необработанные события из raw.events.

    :param pg: Экземпляр PostgresHook с активным соединением.
    :return: Кортеж из двух элементов:
        - Список кортежей — необработанные строки из raw.events.
        - Список строк — имена колонок результата.
    """
    sql = read_sql_file(SQL_SELECT_NEW_EVENTS)
    conn = pg.get_conn()
    cursor = conn.cursor()
    cursor.execute(sql)
    rows = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]
    return rows, columns


def process_event(row: tuple, columns: List[str], pg: PostgresHook,
                  insert_dim_sql: Dict[str, str], insert_fact_sql: str, mark_processed_sql: str) -> bool:
    """
    Валидирует и загружает одно событие в DDS:

    1. Вставка данных в DIM-таблицы (user, product, device, location, date)
    2. Вставка сессии в dim_session
    3. Вставка факта в fact_order
    4. Маркировка события как обработанного

    :param row: Кортеж с данными одной строки события из raw.events.
    :param columns: Список имён колонок, соответствующих данным в row.
    :param pg: Экземпляр PostgresHook для выполнения SQL.
    :param insert_dim_sql: Словарь с SQL-запросами для вставки в DIM таблицы.
    :param insert_fact_sql: SQL-запрос для вставки в FACT таблицу.
    :param mark_processed_sql: SQL-запрос для отметки события как обработанного.
    :return: True, если событие успешно обработано и загружено, иначе False.
    """
    event_dict = dict(zip(columns, row))

    try:
        validated = FactOrder(**event_dict)

        for dim in ["dim_user", "dim_product", "dim_device", "dim_location", "dim_date"]:
            pg.run(insert_dim_sql[dim], parameters=event_dict, autocommit=True)

        pg.run(insert_dim_sql["dim_session"], parameters=event_dict, autocommit=True)

        rows_affected = pg.run(insert_fact_sql, parameters=event_dict, autocommit=True)
        if rows_affected == 0:
            logging.warning(f"[!] Факт по order_id={event_dict['order_id']} не вставлен (возможно, уже существует)")

        pg.run(mark_processed_sql, parameters=(validated.event_id,), autocommit=True)

        return True

    except Exception as e:
        logging.exception(f"[x] Ошибка при обработке event_id={event_dict.get('event_id')}: {e}")
        return False


if __name__ == "__main__":
    pass
