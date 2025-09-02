"""
Модуль dds_prepare_dim

Функции подготовки и обработки dimension данных с учетом staging.
Реализация формирования данных для dim-таблиц, валидации, вставки и получения surrogate key.
"""

import logging
from typing import Any, Dict, List, Optional

from airflow.providers.postgres.hooks.postgres import PostgresHook

from utils.loading.dds.dds_surrogate_keys import get_surrogate_key_for_dim, get_surrogate_key_for_session
from utils.loading.dds.dds_batch_insert import batch_insert
from utils.loading.dds.dds_validate_processing import validate_data
from utils.constants import DIM_LOAD_ORDER, DIM_MODELS


def prepare_dim_data(event: Dict[str, Any], dim_name: str, with_sk: bool = False) -> Optional[Dict[str, Any]]:
    """
    Подготовить словарь данных для конкретной dim-таблицы из события.

    :param event: Словарь с сырыми данными события.
    :param dim_name: Имя dim-таблицы, для которой готовятся данные.
    :param with_sk: Флаг, указывающий подготовку данных с surrogate key (для dim_session).
    :return: Словарь с подготовленными данными или None, если данные неполные/некорректные.
    """
    if dim_name == 'dim_user':
        if not event.get('user_id') or not event.get('user_email'):
            logging.debug(f"prepare_dim_data: пропущен dim_user, нет user_id или user_email для event_id={event.get('event_id')}")
            return None
        return {
            'user_id': event['user_id'],
            'email': event['user_email'],
            'referral_code': event.get('referral_code') or '',
            'user_name': event.get('user_name') or '',
            'birth_date': event.get('birth_date'),
            'profile_created_at': event.get('profile_created_at'),
        }
    elif dim_name == 'dim_product':
        if not event.get('product_id'):
            return None
        return {
            'product_id': event['product_id'],
            'name': event.get('product_name') or '',
            'category': event.get('category_name') or '',
            'supplier': event.get('supplier_name') or '',
            'price': event.get('product_price') or 0,
        }
    elif dim_name == 'dim_device':
        if not event.get('device_type') or not event.get('device_os'):
            return None
        return {
            'device_type': event['device_type'],
            'device_os': event['device_os'],
        }
    elif dim_name == 'dim_location':
        if not event.get('location_country') or not event.get('location_city'):
            return None
        return {
            'country': event['location_country'],
            'city': event['location_city'],
        }
    elif dim_name == 'dim_marketing':
        if not any(event.get(f) for f in ['campaign', 'promocode', 'user_campaign_id']):
            return None
        return {
            'campaign': event.get('campaign'),
            'promocode': event.get('promocode'),
            'user_campaign_id': event.get('user_campaign_id'),
        }
    elif dim_name == 'dim_date':
        if not event.get('event_date'):
            return None
        return {'date': event['event_date']}
    elif dim_name == 'dim_session':
        if not with_sk:
            return None
        required_fields = ['user_id_sk', 'device_id_sk', 'location_id_sk', 'session_start', 'session_end']
        missing = [f for f in required_fields if event.get(f) is None]
        if missing:
            logging.debug(f"[prepare_dim_data] dim_session пропущен, отсутствуют поля {missing} для event_id={event.get('event_id')}")
            return None
        return {
            'session_id': event['session_id'],
            'user_id': event['user_id_sk'],
            'device_id': event['device_id_sk'],
            'location_id': event['location_id_sk'],
            'start_time': event['session_start'],
            'end_time': event['session_end'],
        }
    else:
        logging.error(f"prepare_dim_data: неизвестный dim_name={dim_name}")
        return None


def process_dim_tables(
    pg: PostgresHook,
    batch_dicts: List[Dict[str, Any]],
    insert_dim_sql: Dict[str, str]
) -> List[str]:
    """
    Обработать загрузку данных для всех dim-таблиц (кроме dim_session).

    :param pg: Подключение PostgresHook.
    :param batch_dicts: Список событий в виде словарей.
    :param insert_dim_sql: Словарь с SQL запросами для вставки dim данных.
    :return: Список event_id с ошибками валидации.
    """
    error_ids: List[str] = []

    for dim_name in DIM_LOAD_ORDER:
        if dim_name == 'dim_session':
            continue

        dim_batch = []
        for event in batch_dicts:
            dim_data = prepare_dim_data(event, dim_name)
            if not dim_data:
                continue
            if not validate_data(DIM_MODELS, dim_name, dim_data):
                error_ids.append(str(event.get('event_id')))
                continue
            dim_batch.append(dim_data)

        if dim_batch and insert_dim_sql.get(dim_name):
            batch_insert(pg, insert_dim_sql[dim_name], dim_batch)

    for event in batch_dicts:
        user_key = event.get('user_id')
        event['user_id_sk'] = get_surrogate_key_for_dim('dim_user', user_key, pg) if user_key else None

        device_key = {'device_type': event.get('device_type'), 'device_os': event.get('device_os')}
        event['device_id_sk'] = get_surrogate_key_for_dim('dim_device', device_key, pg) if None not in device_key.values() else None

        location_key = {'country': event.get('location_country'), 'city': event.get('location_city')}
        event['location_id_sk'] = get_surrogate_key_for_dim('dim_location', location_key, pg) if None not in location_key.values() else None

        date_key = event.get('event_date')
        event['date_id_sk'] = get_surrogate_key_for_dim('dim_date', date_key, pg) if date_key else None

        marketing_key = {
            'campaign': event.get('campaign'),
            'promocode': event.get('promocode'),
            'user_campaign_id': event.get('user_campaign_id')
        }
        event['marketing_id_sk'] = get_surrogate_key_for_dim('dim_marketing', marketing_key, pg) if any(marketing_key.values()) else None

        product_id = event.get('product_id')
        event['product_id_sk'] = get_surrogate_key_for_dim('dim_product', product_id, pg) if product_id else None

        logging.debug(
            f"Surrogate keys для event_id={event.get('event_id')}: "
            f"user_id_sk={event.get('user_id_sk')}, device_id_sk={event.get('device_id_sk')}, "
            f"location_id_sk={event.get('location_id_sk')}, date_id_sk={event.get('date_id_sk')}, "
            f"marketing_id_sk={event.get('marketing_id_sk')}, product_id_sk={event.get('product_id_sk')}"
        )
    return error_ids


def process_dim_session(
    pg: PostgresHook,
    batch_dicts: List[Dict[str, Any]],
    insert_dim_sql: Dict[str, str]
) -> List[str]:
    """
    Обработать загрузку данных для dim_session.

    :param pg: Подключение PostgresHook.
    :param batch_dicts: Список событий.
    :param insert_dim_sql: SQL запрос для вставки dim_session.
    :return: Список event_id с ошибками валидации.
    """
    error_ids: List[str] = []

    dim_session_batch = []
    for event in batch_dicts:
        dim_data = prepare_dim_data(event, 'dim_session', with_sk=True)
        if not dim_data:
            logging.debug(f"[process_dim_session] Пропуск dim_session, нет данных для event_id={event.get('event_id')}")
            continue
        if not validate_data(DIM_MODELS, 'dim_session', dim_data):
            logging.error(f"[process_dim_session] Валидация dim_session не пройдена для event_id={event.get('event_id')}")
            error_ids.append(str(event.get('event_id')))
            continue
        dim_session_batch.append(dim_data)

    if dim_session_batch and insert_dim_sql.get('dim_session'):
        logging.info(f"[process_dim_session] Вставка {len(dim_session_batch)} записей в dim_session")
        batch_insert(pg, insert_dim_sql['dim_session'], dim_session_batch)

    for event in batch_dicts:
        if 'session_id' not in event:
            logging.warning(f"[process_dim_session] session_id отсутствует для event_id={event.get('event_id')}")
            continue
        logging.debug(f"[process_dim_session] Получение surrogate key для session_id={event['session_id']} (event_id={event.get('event_id')})")
        sk = get_surrogate_key_for_session(event, pg)
        if sk is None:
            logging.warning(f"[process_dim_session] Surrogate key не найден для session_id={event['session_id']} (event_id={event.get('event_id')})")
        else:
            logging.debug(f"[process_dim_session] Найден surrogate key={sk} для session_id={event['session_id']} (event_id={event.get('event_id')})")
        event['session_id_sk'] = sk
        logging.debug(
            f"Surrogate keys для event_id={event.get('event_id')}: "
            f"user_id_sk={event.get('user_id_sk')}, device_id_sk={event.get('device_id_sk')}, "
            f"location_id_sk={event.get('location_id_sk')}, date_id_sk={event.get('date_id_sk')}, "
            f"marketing_id_sk={event.get('marketing_id_sk')}, product_id_sk={event.get('product_id_sk')}"
        )

    return error_ids


if __name__ == "__main__":
    pass