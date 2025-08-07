"""
Модуль для загрузки и обработки сырых событий из MinIO в raw слой базы данных.

Содержит функции для:
- сериализации специфичных типов данных в JSON,
- получения списка новых файлов из MinIO, которые ещё не были обработаны,
- валидации и батчевой вставки событий в таблицу raw.events,
- записи некорректных событий в raw.events_invalid,
- корректной пометки файлов как обработанных.

Использует Airflow хуки для взаимодействия с MinIO и PostgreSQL.
"""

import json
import logging
from typing import List, Tuple, Optional, Dict, Any
from uuid import UUID
from datetime import datetime, date

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook

from validation.raw_validation import Event
from utils.sql_db.schema_and_tables_init import get_processed_files
from utils.telegram_logger import notify_telegram


def json_serializer(obj: Any) -> str:
    """
    Кастомный сериализатор для JSON, преобразует UUID и datetime в строки.

    :param obj: Объект для сериализации (UUID, datetime, date).
    :return: Строковое представление объекта.
    :raises TypeError: Если тип объекта не поддерживается сериализацией.
    """
    if isinstance(obj, UUID):
        return str(obj)
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError(f"Type {type(obj)} not serializable")


def get_new_files(s3: S3Hook, pg: PostgresHook, bucket: str) -> List[str]:
    """
    Получает список новых необработанных файлов из MinIO.

    :param s3: Инстанс S3Hook для работы с MinIO.
    :param pg: Инстанс PostgresHook для работы с БД.
    :param bucket: Название бакета MinIO.
    :return: Список новых файлов для обработки.
    """
    processed_files = get_processed_files(pg)
    files = s3.list_keys(bucket_name=bucket) or []
    return [f for f in files if f not in processed_files]


def parse_file(file_key: str, s3: S3Hook, bucket: str) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
    """
    Читает и валидирует файл из MinIO.

    :param file_key: Имя файла в MinIO.
    :param s3: Инстанс S3Hook.
    :param bucket: Название бакета MinIO.
    :return: Кортеж (row_dict для вставки в raw.events, error_dict для вставки в raw.events_invalid).
    """
    data: Optional[dict] = None
    try:
        content = s3.read_key(bucket_name=bucket, key=file_key)
        if isinstance(content, bytes):
            content = content.decode("utf-8")

        data = json.loads(content)
        validated = Event(**data)

        # Преобразуем продукты и исходный payload в JSONB
        products_jsonb = None
        if validated.products:
            products_jsonb = json.dumps([prod.dict() for prod in validated.products], default=json_serializer)

        raw_payload_jsonb = json.dumps(data, default=json_serializer)

        row = {
            "event_id": validated.event_id,
            "event_type": validated.event_type,
            "event_time": validated.event_time,
            "event_date": validated.event_date,

            "user_id": validated.user.user_id,
            "email": validated.user.email,
            "referral_code": validated.user.referral_code,
            "user_name": validated.user.profile.name,
            "birth_date": validated.user.profile.birth_date,
            "profile_created_at": validated.user.profile.created_at,

            "session_id": validated.session.session_id,
            "session_start_time": validated.session.start_time,
            "session_end_time": validated.session.end_time,
            "device_type": validated.session.device.type,
            "device_os": validated.session.device.os,
            "location_country": validated.session.device.location.country,
            "location_city": validated.session.device.location.city,
            "pages_viewed": validated.session.pages_viewed,

            "products": products_jsonb,

            "order_id": validated.order.order_id if validated.order else None,
            "payment_id": validated.order.payment_id if validated.order else None,
            "order_items": validated.order.order_items if validated.order else None,
            "total_amount": validated.order.total_amount if validated.order else None,
            "order_status": validated.order.status if validated.order else None,

            "campaign": validated.marketing.campaign if validated.marketing and validated.marketing.campaign else None,
            "promocode": validated.marketing.promocode if validated.marketing and validated.marketing.promocode else None,
            "user_campaign_id": validated.marketing.user_campaign_id if validated.marketing else None,

            "raw_payload": raw_payload_jsonb,
        }
        return row, None

    except Exception as e:
        logging.error(f"Ошибка при разборе файла {file_key}: {e}")
        error_row = {
            "file_name": file_key,
            "event_id": data.get("event_id") if isinstance(data, dict) else None,
            "event_time": data.get("event_time") if isinstance(data, dict) else None,
            "error_message": str(e),
            "raw_payload": json.dumps(data) if isinstance(data, (dict, list)) else str(data),
        }
        return None, error_row


def insert_batch(
    pg: PostgresHook,
    table: str,
    rows: List[Dict[str, Any]],
    columns: List[str],
    batch_type: str
) -> bool:
    """
    Вставляет батч строк в указанную таблицу с защитой от падения DAG.

    :param pg: Хук Postgres.
    :param table: Имя таблицы.
    :param rows: Список строк (dict).
    :param columns: Список колонок в порядке вставки.
    :param batch_type: Тип батча (для логов: events / invalid).
    :return: True, если вставка успешна, False — если произошла ошибка.
    """
    try:
        pg.insert_rows(
            table=table,
            rows=[tuple(r[col] for col in columns) for r in rows],
            target_fields=columns
        )
        logging.info(f"[BATCH OK] Вставлено {len(rows)} строк в {table}")
        return True
    except Exception as e:
        logging.error(f"[BATCH FAIL] Ошибка при вставке {batch_type} батча: {e}")
        notify_telegram(f"[x] Ошибка вставки батча в {table}: {e}")
        return False


if __name__ == "__main__":
    pass
