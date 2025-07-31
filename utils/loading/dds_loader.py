import os
import logging
from airflow.providers.postgres.hooks.postgres import PostgresHook
from utils.validation.dds_validation import FactOrder
from utils.sql_db.sql_utils import read_sql_file, run_select_with_columns
from typing import List, Dict, Optional
from utils.loading.dds_surrogate_keys import get_surrogate_key
from utils.constants import DIM_LOAD_ORDER, FACT_LOAD_ORDER
from utils.sql_db.sql_paths import BASE_SQL_DDS_INSERT, SQL_SELECT_NEW_EVENTS

def fetch_new_raw_events(pg: PostgresHook) -> Tuple[List[tuple], List[str]]:
    """
    Получает новые сырые события из базы данных.

    :param pg: Экземпляр PostgresHook с соединением.
    :return: Кортеж из списка кортежей с данными и списка названий колонок.
    """
    return run_select_with_columns(pg, SQL_SELECT_NEW_EVENTS)

def load_data_in_order(
    event_dict: Dict[str, object],
    pg: PostgresHook,
    insert_sql: Dict[str, str],
    load_order: Optional[List[str]] = None,
) -> None:
    """
    Универсальная функция для загрузки данных в таблицы по заданному порядку.

    :param event_dict: Словарь с данными события для вставки.
    :param pg: Инстанс PostgresHook для выполнения SQL.
    :param insert_sql: Словарь с SQL-запросами для вставки.
    :param load_order: Опциональный список имён таблиц для загрузки в нужном порядке.
                       Если None — загрузка по ключам словаря insert_sql (без порядка).
    :return: None
    """
    targets = load_order if load_order is not None else list(insert_sql.keys())

    for target in targets:
        sql = insert_sql.get(target)
        if sql:
            try:
                pg.run(sql, parameters=event_dict, autocommit=True)
                logging.info(f"Данные вставлены в таблицу {target}.")
            except Exception as e:
                logging.error(f"Ошибка при вставке в таблицу {target}: {e}")
                raise
        else:
            logging.warning(f"SQL для таблицы {target} не найден.")


def load_dim_data(event_dict: Dict[str, object], pg: PostgresHook, insert_dim_sql: Dict[str, str]) -> None:
    """
    Загружает данные в измерительные таблицы (DIM) согласно установленному порядку.

    :param event_dict: Словарь с данными события.
    :param pg: Экземпляр PostgresHook для выполнения запросов.
    :param insert_dim_sql: Словарь с SQL-запросами для вставки в измерительные таблицы.
    :return: None
    """
    load_data_in_order(event_dict, pg, insert_dim_sql, DIM_LOAD_ORDER)


def load_fact_data(event_dict: Dict[str, object], pg: PostgresHook, insert_fact_sql: Dict[str, str]) -> None:
    """
    Загружает данные в фактографические таблицы (FACT) согласно установленному порядку.

    :param event_dict: Словарь с данными события.
    :param pg: Экземпляр PostgresHook для выполнения запросов.
    :param insert_fact_sql: Словарь с SQL-запросами для вставки в фактографические таблицы.
    :return: None
    """
    load_data_in_order(event_dict, pg, insert_fact_sql, FACT_LOAD_ORDER)



def load_sql_scripts(directory: str, prefix: str) -> Dict[str, str]:
    """
    Загружает все SQL-скрипты из указанной папки с фильтром по префиксу имени файла.

    :param directory: Путь к папке с SQL-файлами.
    :param prefix: Префикс имени файла (например, 'insert_dim_' или 'insert_fact_').
    :return: Словарь {имя_таблицы: SQL-код}.
    """
    sql_dict = {}
    try:
        for filename in os.listdir(directory):
            if filename.startswith(prefix) and filename.endswith('.sql'):
                name = filename.replace(prefix, '').replace('.sql', '')
                path = os.path.join(directory, filename)
                sql_dict[name] = read_sql_file(path)
    except Exception as e:
        logging.error(f"Ошибка при загрузке SQL из {directory} с префиксом {prefix}: {e}")
        raise
    return sql_dict

def get_insert_dim_sql() -> Dict[str, str]:
    """
    Загружает SQL-скрипты вставки для измерительных (DIM) таблиц из базового каталога.

    :return: Словарь, где ключ — имя таблицы, значение — SQL-запрос вставки.
    """
    return load_sql_scripts(BASE_SQL_DDS_INSERT, 'insert_dim_')


def get_insert_fact_sql() -> Dict[str, str]:
    """
    Загружает SQL-скрипты вставки для фактографических (FACT) таблиц из базового каталога.

    :return: Словарь, где ключ — имя таблицы, значение — SQL-запрос вставки.
    """
    return load_sql_scripts(BASE_SQL_DDS_INSERT, 'insert_fact_')


def process_event(
    row: tuple,
    columns: List[str],
    pg: PostgresHook,
    insert_dim_sql: Dict[str, str],
    insert_fact_sql: Dict[str, str],
    mark_processed_sql: str
) -> bool:
    """
    Обработка одного события из raw.events:
    1. Вставка в DIM (только если есть данные)
    2. Получение surrogate keys
    3. Валидация через FactOrder
    4. Вставка в FACT
    5. Отметка как обработанное
    """
    event_dict = dict(zip(columns, row))
    raw_event_id = event_dict.get('event_id')

    try:
        # 1. Вставка в DIM (только при наличии нужных полей)
        for dim, sql in insert_dim_sql.items():
            if dim == 'dim_product' and not event_dict.get('product_id'):
                continue
            pg.run(sql, parameters=event_dict, autocommit=True)

        # 2. Получение surrogate keys
        surrogate_map = {
            'user_id': ('dim_user', event_dict.get('user_id')),
            'product_id': ('dim_product', event_dict.get('product_id')),
            'device_id': ('dim_device', event_dict.get('device_type')),
            'location_id': ('dim_location', event_dict.get('location_city')),
            'date_id': ('dim_date', event_dict.get('event_date')),
            'session_id': ('dim_session', event_dict.get('session_id')),
            'marketing_id': ('dim_marketing', event_dict.get('user_campaign_id')),
        }

        for field, (table, raw_value) in surrogate_map.items():
            if raw_value:
                event_dict[field] = get_surrogate_key(table, raw_value, pg)
            else:
                event_dict[field] = None

        # 3. Валидация через Pydantic
        required_keys = ['user_id', 'device_id', 'location_id', 'date_id', 'session_id']
        if any(event_dict.get(k) is None for k in required_keys):
            raise ValueError(f"Отсутствуют обязательные surrogate keys для event_id={raw_event_id}")

        FactOrder(**event_dict)

        # 4. Вставка в FACT
        for fact, sql in insert_fact_sql.items():
            pg.run(sql, parameters=event_dict, autocommit=True)

        # 5. Отметка события как обработанного
        pg.run(mark_processed_sql, parameters=(raw_event_id,), autocommit=True)

        return True

    except Exception as e:
        logging.exception(f"Ошибка при обработке event_id={raw_event_id}: {e}")
        return False





if __name__ == "__main__":
    pass
