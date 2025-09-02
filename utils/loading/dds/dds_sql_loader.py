"""
Модуль dds_sql_loader

Содержит функции для загрузки SQL скриптов из файловой системы и выборки новых событий из staging-слоя.

Функции обеспечивают загрузку SQL-инструкций для загрузки dimension и fact таблиц, а также выборку новых данных для обработки.
"""

import os
import logging
from typing import Dict, List, Tuple

from airflow.providers.postgres.hooks.postgres import PostgresHook

from utils.sql_db.sql_utils import read_sql_file, run_select_with_columns
from utils.sql_db.sql_paths import BASE_SQL_DDS_INSERT, SQL_SELECT_NEW_STAGING_EVENTS

def load_sql_scripts(directory: str, prefix: str) -> Dict[str, str]:
    """
    Загружает SQL-скрипты из указанной директории, фильтруя по префиксу в названии файлов.

    :param directory: путь к директории с SQL файлами
    :param prefix: префикс имени файла для фильтрации
    :return: словарь, где ключ — имя скрипта без префикса и расширения, значение — содержимое SQL
    :raises Exception: в случае ошибок чтения файлов или директории
    """
    sql_dict: Dict[str, str] = {}
    try:
        for f in os.listdir(directory):
            if f.startswith(prefix) and f.endswith('.sql'):
                name = f[len(prefix):-4]
                full_path = os.path.join(directory, f)
                logging.info(f"Загрузка SQL: {full_path} -> ключ: {name}")
                sql_dict[name] = read_sql_file(full_path)
    except Exception as e:
        logging.error(f"Ошибка загрузки SQL из {directory} с префиксом {prefix}: {e}")
        raise
    return sql_dict


def get_insert_dim_sql() -> Dict[str, str]:
    """
    Получает словарь SQL запросов для вставки данных в dimension таблицы.

    :return: словарь с ключами вида 'dim_<имя_таблицы>' и SQL запросами на вставку
    """
    sqls = load_sql_scripts(BASE_SQL_DDS_INSERT, 'insert_dim_')
    return {f"dim_{k}": v for k, v in sqls.items()}


def get_insert_fact_sql() -> Dict[str, str]:
    """
    Получает словарь SQL запросов для вставки данных в fact таблицы.

    :return: словарь с ключами вида 'fact_<имя_таблицы>' и SQL запросами на вставку
    """
    sqls = load_sql_scripts(BASE_SQL_DDS_INSERT, 'insert_fact_')
    result = {f"fact_{k}": v for k, v in sqls.items()}
    logging.info(f"Загруженные FACT SQL: {list(result.keys())}")
    return result


def fetch_new_staging_events(pg: PostgresHook) -> Tuple[List[Tuple], List[str]]:
    """
    Выбирает новые события из staging слоя для последующей обработки.

    :param pg: хук подключения к Postgres
    :return: кортеж из списка строк результата и списка названий колонок
    """
    return run_select_with_columns(pg, SQL_SELECT_NEW_STAGING_EVENTS)


if __name__ == "__main__":
    pass