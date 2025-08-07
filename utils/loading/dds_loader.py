import os
import logging
from typing import Any, Dict, List, Tuple, Optional
from airflow.providers.postgres.hooks.postgres import PostgresHook
from pydantic import ValidationError

from utils.validation.dds_validation import (
    DimUser, DimProduct, DimDevice, DimLocation, DimMarketing, DimSession,
    FactEvent, FactOrder, FactOrderItem, FactSession
)
from utils.sql_db.sql_utils import read_sql_file, run_select_with_columns
from utils.loading.dds_surrogate_keys import get_surrogate_key_for_dim, get_surrogate_key_for_session
from utils.constants import DIM_LOAD_ORDER, FACT_LOAD_ORDER
from utils.sql_db.sql_paths import BASE_SQL_DDS_INSERT, SQL_SELECT_NEW_STAGING_EVENTS

BATCH_SIZE = 50

# 1. Загрузка SQL

def load_sql_scripts(directory: str, prefix: str) -> Dict[str, str]:
    sql_dict = {}
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
    sqls = load_sql_scripts(BASE_SQL_DDS_INSERT, 'insert_dim_')
    return {f"dim_{k}": v for k, v in sqls.items()}

def get_insert_fact_sql() -> Dict[str, str]:
    sqls = load_sql_scripts(BASE_SQL_DDS_INSERT, 'insert_fact_')
    result = {f"fact_{k}": v for k, v in sqls.items()}
    logging.info(f"Загруженные FACT SQL: {list(result.keys())}")
    return result

def fetch_new_staging_events(pg: PostgresHook) -> Tuple[List[Tuple], List[str]]:
    return run_select_with_columns(pg, SQL_SELECT_NEW_STAGING_EVENTS)

# 2. Подготовка DIM данных с учётом staging

def prepare_dim_data(event: Dict[str, Any], dim_name: str, with_sk: bool = False) -> Optional[Dict[str, Any]]:
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

# 3. Валидация

DIM_MODELS = {
    "dim_user": DimUser,
    "dim_product": DimProduct,
    "dim_device": DimDevice,
    "dim_location": DimLocation,
    "dim_marketing": DimMarketing,
    "dim_session": DimSession,
}

FACT_MODELS = {
    "fact_event": FactEvent,
    "fact_order": FactOrder,
    "fact_order_item": FactOrderItem,
    "fact_session": FactSession,
}

def validate_data(model_dict: Dict[str, Any], model_name: str, data: Dict[str, Any]) -> bool:
    model = model_dict.get(model_name)
    if not model:
        logging.warning(f"validate_data: модель для {model_name} не найдена, пропускаем валидацию")
        return True
    try:
        model(**data)
        return True
    except ValidationError as e:
        logging.error(f"Валидация {model_name} не пройдена: {e}")
        return False

# 4. batch insert

def batch_insert(pg: PostgresHook, sql: str, data: List[Dict[str, Any]]) -> None:
    if not data:
        logging.debug("batch_insert: данных для вставки нет")
        return
    conn = pg.get_conn()
    try:
        with conn.cursor() as cur:
            columns = list(data[0].keys())
            values = [tuple(d[c] for c in columns) for d in data]
            logging.debug(f"batch_insert: Выполняется вставка {len(data)} записей с колонками {columns}")
            cur.executemany(sql, values)
        conn.commit()
        logging.info(f"batch_insert: успешно вставлено {len(data)} записей")
    except Exception as e:
        logging.error(f"batch_insert: ошибка вставки: {e}", exc_info=True)
        raise

# 6. Тип факта (доработанный)

def get_all_fact_types(event: Dict[str, Any]) -> List[str]:
    types = []
    if event.get('order_id') and event.get('payment_id'):
        types.append('fact_order')
        if event.get('product_id') and event.get('product_quantity'):
            types.append('fact_order_item')
    if event.get('session_id'):
        types.append('fact_session')
    types.append('fact_event')  # всегда как базовый
    return types

# 6.1 Подготовка данных для фактов

def prepare_fact_data(event: Dict[str, Any], fact_type: str, pg: PostgresHook) -> Optional[Dict[str, Any]]:
    """
    Подготовка данных для загрузки в факт-таблицы.

    :param event: Словарь события
    :param fact_type: Тип факт-таблицы
    :param pg: Хук подключения к PostgreSQL
    :return: Словарь с подготовленными данными или None, если данные некорректны
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


# 7. Обработка DIM таблиц (кроме dim_session)

def process_dim_tables(pg, batch_dicts, insert_dim_sql) -> List[str]:
    error_ids = []

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


# 8. Обработка dim_session

def process_dim_session(
    pg: PostgresHook,
    batch_dicts: List[Dict[str, Any]],
    insert_dim_sql: Dict[str, str]
) -> List[str]:
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

# 9. Обработка фактов

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


# 10. Обработка батча

def process_batch(
    batch_rows: List[Tuple],
    columns: List[str],
    pg: PostgresHook,
    insert_dim_sql: Dict[str, str],
    insert_fact_sql: Dict[str, str],
    mark_processed_sql: str
) -> Tuple[int, List[str]]:
    batch_dicts = [dict(zip(columns, row)) for row in batch_rows]

    errors_dim = process_dim_tables(pg, batch_dicts, insert_dim_sql)
    errors_session = process_dim_session(pg, batch_dicts, insert_dim_sql)
    errors_fact = process_fact_tables(pg, batch_dicts, insert_fact_sql)

    error_ids = list(set(errors_dim + errors_session + errors_fact))

    processed_ids = [(d['stg_id'],) for d in batch_dicts if str(d.get('event_id')) not in error_ids]
    if processed_ids:
        conn = pg.get_conn()
        with conn.cursor() as cur:
            cur.executemany(mark_processed_sql, processed_ids)
        conn.commit()

    success_count = len(batch_dicts) - len(error_ids)
    logging.info(f"Батч обработан: успех={success_count}, ошибок={len(error_ids)}")
    return success_count, error_ids

# 11. Главная функция

def load_staging_events_batch(
    pg: PostgresHook,
    insert_dim_sql: Dict[str, str],
    insert_fact_sql: Dict[str, str],
    mark_processed_sql: str,
) -> Tuple[int, List[str]]:
    rows, columns = fetch_new_staging_events(pg)
    if not rows:
        logging.info("Нет новых событий для обработки.")
        return 0, []

    total_success = 0
    total_errors: List[str] = []
    for i in range(0, len(rows), BATCH_SIZE):
        batch = rows[i : i + BATCH_SIZE]
        success, errors = process_batch(batch, columns, pg, insert_dim_sql, insert_fact_sql, mark_processed_sql)
        total_success += success
        total_errors.extend(errors)
        logging.info(f"Итоги обработки батча: успешно вставлено {success} событий, ошибок: {len(errors)}")

    return total_success, total_errors
