"""
Модуль dds_surrogate_keys

Функции для получения surrogate key (SK) для dimension таблиц и сессий.
Реализованы универсальные методы поиска SK по ключам и вставки новых записей при необходимости.
"""

import logging
from typing import Optional, Union, Dict, Any, List, Tuple
from uuid import UUID

from airflow.providers.postgres.hooks.postgres import PostgresHook

# Конфигурация ключевых колонок по таблицам DIM
DIM_KEYS: Dict[str, List[str]] = {
    'dim_date': ['date'],
    'dim_device': ['device_type', 'device_os'],
    'dim_location': ['country', 'city'],
    'dim_marketing': ['campaign', 'promocode', 'user_campaign_id'],
    'dim_product': ['product_id'],
    'dim_user': ['user_id'],
}


def build_where_clause(columns: List[str], values: Dict[str, Any]) -> Tuple[str, List[Any]]:
    """
    Построить WHERE-условие с поддержкой сравнения NULL значений.

    :param columns: Список колонок для условия.
    :param values: Словарь значений для сравнения.
    :return: Кортеж из SQL-условия (строка) и списка параметров.
    """
    clauses = []
    params = []
    for col in columns:
        val = values.get(col)
        clauses.append(f"({col} = %s OR ({col} IS NULL AND %s IS NULL))")
        params.extend([val, val])
    return " AND ".join(clauses), params


def insert_dim_record(
    table: str,
    raw_values: Dict[str, Any],
    pg: PostgresHook
) -> Optional[int]:
    """
    Вставить новую запись в dimension таблицу и вернуть surrogate key.

    :param table: Имя dimension таблицы.
    :param raw_values: Данные для вставки (колонки и значения).
    :param pg: Подключение PostgresHook.
    :return: surrogate key (id) или None при ошибке.
    """
    columns = list(raw_values.keys())
    values = [raw_values[col] for col in columns]
    placeholders = ', '.join(['%s'] * len(columns))
    columns_str = ', '.join(columns)

    sql = f"INSERT INTO dds.{table} ({columns_str}) VALUES ({placeholders}) RETURNING id"

    try:
        result = pg.get_first(sql, parameters=tuple(values))
        if result:
            logging.debug(f"insert_dim_record: вставлен id={result[0]} в таблицу {table}")
            return result[0]
        else:
            logging.error(f"insert_dim_record: вставка не удалась для таблицы {table} и значений {raw_values}")
            return None
    except Exception as e:
        logging.error(f"Ошибка при вставке в {table}: {e}")
        return None


def get_surrogate_key_for_dim(
    table: str,
    raw_values: Union[str, UUID, Dict[str, Any]],
    pg: PostgresHook,
    insert_if_not_found: bool = True
) -> Optional[int]:
    """
    Получить surrogate key для dimension таблицы.
    Если запись не найдена, при insert_if_not_found=True вставляет новую.

    :param table: Имя dimension таблицы.
    :param raw_values: Значения для поиска (словарь) или одиночное значение (для single-key таблиц).
    :param pg: Подключение PostgresHook.
    :param insert_if_not_found: Вставлять новую запись при отсутствии.
    :return: surrogate key (id) или None.
    :raises ValueError: если table не поддерживается или raw_values некорректны.
    """
    if raw_values is None:
        logging.debug(f"get_surrogate_key: raw_values=None для таблицы {table}, возвращаем None")
        return None

    if table not in DIM_KEYS:
        raise ValueError(f"Таблица {table} не поддерживается")

    columns = DIM_KEYS[table]

    if not isinstance(raw_values, dict):
        if len(columns) == 1:
            raw_values = {columns[0]: raw_values}
        else:
            raise ValueError(f"Для таблицы {table} требуется словарь с ключами {columns}")

    where_clause, params = build_where_clause(columns, raw_values)
    sql = f"SELECT id FROM dds.{table} WHERE {where_clause}"

    logging.debug(f"get_surrogate_key SQL: {sql} | params={params}")

    try:
        result = pg.get_first(sql, parameters=tuple(params))
        if result:
            logging.debug(f"get_surrogate_key: найден id={result[0]} для таблицы {table} и ключей {raw_values}")
            return result[0]
        elif insert_if_not_found:
            return insert_dim_record(table, raw_values, pg)
        else:
            logging.debug(f"get_surrogate_key: запись не найдена и вставка отключена для {table} и {raw_values}")
            return None
    except Exception as e:
        logging.error(f"Ошибка в get_surrogate_key для таблицы {table}, ключи {raw_values}: {e}")
        return None


def insert_session_record(
    raw_values: Dict[str, Any],
    pg: PostgresHook
) -> Optional[int]:
    """
    Вставить новую запись в dim_session и вернуть surrogate key.

    :param raw_values: Данные для вставки.
    :param pg: Подключение PostgresHook.
    :return: surrogate key (id) или None при ошибке.
    """
    columns = list(raw_values.keys())
    values = [raw_values[col] for col in columns]
    placeholders = ', '.join(['%s'] * len(columns))
    columns_str = ', '.join(columns)

    sql = f"INSERT INTO dds.dim_session ({columns_str}) VALUES ({placeholders}) RETURNING id"

    try:
        result = pg.get_first(sql, parameters=tuple(values))
        if result:
            logging.debug(f"insert_session_record: вставлен id={result[0]} для session_id={raw_values.get('session_id')}")
            return result[0]
        else:
            logging.error(f"insert_session_record: вставка не удалась для session_id={raw_values.get('session_id')}")
            return None
    except Exception as e:
        logging.error(f"Ошибка при вставке в dim_session: {e}")
        return None


def get_surrogate_key_for_session(
    raw_values: Dict[str, Any],
    pg: PostgresHook,
    insert_if_not_found: bool = True
) -> Optional[int]:
    """
    Получить surrogate key для dim_session по session_id.
    Если не найден, при insert_if_not_found=True вставляет новую запись.

    :param raw_values: Данные с ключом 'session_id' и другими полями.
    :param pg: Подключение PostgresHook.
    :param insert_if_not_found: Вставлять новую запись при отсутствии.
    :return: surrogate key (id) или None.
    :raises ValueError: если в raw_values отсутствует 'session_id'.
    """
    if 'session_id' not in raw_values:
        raise ValueError("Отсутствует ключ 'session_id' в raw_values")

    sql = "SELECT id FROM dds.dim_session WHERE session_id = %s"
    params = (raw_values['session_id'],)

    logging.debug(f"get_surrogate_key_session SQL: {sql} | params={params}")

    try:
        result = pg.get_first(sql, parameters=params)
        if result:
            logging.debug(f"Найден surrogate key {result[0]} для session_id {raw_values['session_id']}")
            return result[0]
        elif insert_if_not_found:
            return insert_session_record(raw_values, pg)
        else:
            logging.debug(f"Не найден surrogate key и вставка отключена для session_id {raw_values['session_id']}")
            return None
    except Exception as e:
        logging.error(f"Ошибка при получении surrogate key для session_id {raw_values.get('session_id')}: {e}")
        return None


if __name__ == "__main__":
    pass