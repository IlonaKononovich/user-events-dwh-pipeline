"""
Модуль dds_prepare_fact

Функции определения типов факт-таблиц и подготовки данных для загрузки в факт-таблицы.
"""

import logging
from typing import Any, Dict, List, Optional
from airflow.providers.postgres.hooks.postgres import PostgresHook
from utils.constants import FACT_LOAD_ORDER, FACT_MODELS 
from utils.loading.dds.dds_validate_processing import validate_data
from utils.loading.dds.dds_batch_insert import batch_insert


def get_all_fact_types(event: Dict[str, Any]) -> List[str]:
    """
    Определить список всех типов факт-таблиц, к которым относится событие.

    :param event: Словарь с данными события.
    :return: Список строк с типами факт-таблиц.
    """
    types: List[str] = []
    if event.get('order_id') and event.get('payment_id'):
        types.append('fact_order')
        if event.get('product_id') and event.get('product_quantity'):
            types.append('fact_order_item')
    if event.get('session_id'):
        types.append('fact_session')
    types.append('fact_event')  # базовый факт всегда присутствует
    return types


def prepare_fact_data(event: Dict[str, Any], fact_type: str, pg: PostgresHook) -> Optional[Dict[str, Any]]:
    """
    Подготовить словарь данных для загрузки в факт-таблицу заданного типа.

    :param event: Словарь события.
    :param fact_type: Тип факт-таблицы.
    :param pg: PostgresHook для подключения к базе.
    :return: Словарь с подготовленными данными или None при ошибках/некорректных данных.
    """
    logging.info(f"[prepare_fact_data] Обработка fact_type={fact_type} для event_id={event.get('event_id')}")

    if fact_type == 'fact_order':
        return {
            'order_id': event['order_id'],
            'payment_id': event['payment_id'],
            'order_items': event.get('order_items', 0),
            'total_amount': event.get('total_amount', 0),
            'order_status': event.get('order_status'),
            'user_id': event['user_id_sk'],
            'session_id': event['session_id_sk'],
            'device_id': event['device_id_sk'],
            'location_id': event['location_id_sk'],
            'date_id': event['date_id_sk'],
            'marketing_id': event.get('marketing_id_sk'),
            'event_time': event.get('event_time'),
            'raw_event_id': event.get('event_id'),
        }

    elif fact_type == 'fact_order_item':
        logging.info(f"DEBUG: Событие для fact_order_item: {event}")
        if not event.get('order_id'):
            logging.warning("prepare_fact_data: отсутствует order_id для fact_order_item")
            return None
        if not event.get('product_id_sk'):
            logging.warning(f"prepare_fact_data: отсутствует product_id_sk для события {event.get('event_id')}")
            return None
        if event.get('product_quantity') is None or event['product_quantity'] <= 0:
            logging.warning(f"prepare_fact_data: некорректное количество товара: {event.get('product_quantity')}")
            return None
        if event.get('product_price') is None or event['product_price'] < 0:
            logging.warning(f"prepare_fact_data: некорректная цена товара: {event.get('product_price')}")
            return None

        return {
            'order_id': event['order_id'],
            'product_id': event['product_id_sk'],
            'quantity': event['product_quantity'],
            'price': event['product_price'],
        }

    elif fact_type == 'fact_session':
        return {
            'session_dim_id': event['session_id_sk'],
            'date_id': event['date_id_sk'],
            'session_start_time': event.get('start_time') or event.get('session_start'),
            'session_end_time': event.get('end_time') or event.get('session_end'),
            'pages_viewed': event.get('pages_viewed'),
            'events_count': event.get('events_count', 0),
            'is_converted': event.get('is_converted', False),
        }

    elif fact_type == 'fact_event':
        return {
            'event_id': event['event_id'],
            'event_type': event['event_type'],
            'event_time': event['event_time'],
            'event_date': event['event_date'],

            'user_id': event['user_id_sk'],
            'session_id': event['session_id_sk'],
            'device_id': event['device_id_sk'],
            'location_id': event['location_id_sk'],
            'date_id': event['date_id_sk'],

            'marketing_id': event.get('marketing_id_sk'),
            'pages_viewed': event.get('pages_viewed'),
            'products': event.get('products'),

            'order_id': event.get('order_id'),
            'payment_id': event.get('payment_id'),
            'order_items': event.get('order_items'),
            'total_amount': event.get('total_amount'),
            'order_status': event.get('order_status'),
        }

    else:
        logging.error(f"prepare_fact_data: неизвестный fact_type={fact_type}")
        return None
    

def process_fact_tables(
    pg: PostgresHook,
    batch_dicts: List[Dict[str, Any]],
    insert_fact_sql: Dict[str, str]
) -> List[str]:
    """
    Обработка и вставка записей в fact-таблицы.

    :param pg: подключение к Postgres через Airflow Hook
    :param batch_dicts: список исходных записей из staging/dwh
    :param insert_fact_sql: словарь с SQL-инструкциями по загрузке
    :return: список event_id с ошибками
    """
    error_ids: List[str] = []
    batches: Dict[str, List[Dict[str, Any]]] = {fact: [] for fact in FACT_LOAD_ORDER}

    for event in batch_dicts:
        eid = str(event.get('event_id'))

        try:
            event['user_id'] = event['user_id_sk']
            event['device_id'] = event['device_id_sk']
            event['location_id'] = event['location_id_sk']
            event['date_id'] = event['date_id_sk']
            event['session_id'] = event['session_id_sk']
        except KeyError as e:
            logging.warning(f"process_fact_tables: отсутствует surrogate key {e} для event_id={eid}")
            error_ids.append(eid)
            continue

        required_sks = ['user_id', 'device_id', 'location_id', 'date_id', 'session_id']
        if any(event.get(sk) is None for sk in required_sks):
            logging.warning(f"process_fact_tables: отсутствует обязательный surrogate key для event_id={eid}")
            error_ids.append(eid)
            continue

        fact_types = get_all_fact_types(event)
        for fact_type in fact_types:
            fact_data = prepare_fact_data(event, fact_type, pg)
            if fact_data:
                logging.info(f"Prepared fact data для event_id={eid} в {fact_type}: {fact_data}")
            else:
                logging.warning(f"prepare_fact_data вернул None для event_id={eid} в {fact_type}")
                error_ids.append(eid)
                continue

            if not validate_data(FACT_MODELS, fact_type, fact_data):
                error_ids.append(eid)
                continue

            batches[fact_type].append(fact_data)

    logging.info(f"Количество подготовленных записей для fact_order: {len(batches.get('fact_order', []))}")

    for fact_name in FACT_LOAD_ORDER:
        batch = batches.get(fact_name, [])
        if insert_fact_sql.get(fact_name) and batch:
            logging.info(f"[process_fact_tables] Вставка в {fact_name}: {len(batch)} записей")
            logging.debug(f"[process_fact_tables] SQL для {fact_name}: {insert_fact_sql[fact_name]}")
            logging.debug(f"[process_fact_tables] Пример данных для вставки: {batch[:2]}")
            batch_insert(pg, insert_fact_sql[fact_name], batch)
        else:
            logging.info(f"[process_fact_tables] Пропуск вставки для {fact_name}: нет данных или SQL")

    return error_ids


if __name__ == "__main__":
    pass