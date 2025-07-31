"""
Модуль для загрузки и обработки сырых событий из MinIO в raw слой базы данных.

Содержит функции для:
- сериализации специфичных типов данных в JSON,
- получения списка новых файлов из MinIO, которые ещё не были обработаны,
- валидации и вставки событий в таблицу raw.events,
- обработки ошибок и записи некорректных событий в raw.events_invalid,
- отметки файлов как обработанных.

Использует Airflow хуки для взаимодействия с MinIO и PostgreSQL.
"""

import json
import logging
from typing import List, Tuple, Optional
from uuid import UUID
from datetime import datetime, date
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook

from validation.raw_validation import Event
from utils.loading.raw_processed_files import mark_file_as_processed, get_processed_files

from sql_db.sql_paths import SQL_INSERT_EVENT_INVALID


def json_serializer(obj) -> str:
    """
    Кастомный сериализатор для JSON, преобразует UUID и datetime в строки.

    :param obj: Объект для сериализации (может быть UUID, datetime, date).
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


def process_file(file_key: str, s3: S3Hook, pg: PostgresHook, insert_sql: str, bucket: str) -> Tuple[bool, str]:
    """
    Обрабатывает один файл: читает, валидирует, формирует параметры для вставки,
    записывает в raw.events, при ошибке пишет в raw.events_invalid и отмечает файл как обработанный.

    :param file_key: Имя файла в MinIO.
    :param s3: Инстанс S3Hook.
    :param pg: Инстанс PostgresHook.
    :param insert_sql: SQL-запрос для вставки события.
    :param bucket: Название бакета MinIO.
    :return: Кортеж (успешно ли обработан, сообщение с ошибкой или пустая строка).
    """
    data: Optional[dict] = None
    try:
        content = s3.read_key(bucket_name=bucket, key=file_key)
        if isinstance(content, bytes):
            content = content.decode("utf-8")

        data = json.loads(content)
        validated = Event(**data)

        products_jsonb = None
        if validated.products:
            products_list = [prod.dict() for prod in validated.products]
            products_jsonb = json.dumps(products_list, default=json_serializer)

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

        pg.run(insert_sql, parameters=row)
        mark_file_as_processed(pg, file_key)
        return True, ""

    except Exception as e:
        logging.exception(f"Ошибка при обработке файла {file_key}")

        event_id = None
        event_time = None
        try:
            if isinstance(data, dict):
                event_id = data.get("event_id")
                event_time = data.get("event_time")
        except Exception:
            pass

        try:
            pg.run(
                SQL_INSERT_EVENT_INVALID,
                parameters=(
                    file_key,
                    event_id,
                    event_time,
                    str(e),
                    json.dumps(data) if isinstance(data, (dict, list)) else str(data),
                ),
            )
            mark_file_as_processed(pg, file_key)
        except Exception as db_err:
            logging.error(f"Не удалось записать файл {file_key} в events_invalid: {db_err}")

        return False, f"{file_key}: {e}"


if __name__ == "__main__":
    pass
