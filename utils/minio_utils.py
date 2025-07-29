import json
import logging
from typing import List, Tuple
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from utils.raw_validation import Event
from utils.db_utils import mark_file_as_processed, get_processed_files

def get_new_files(s3: S3Hook, pg: PostgresHook, bucket: str) -> List[str]:
    """
    Получает список новых необработанных файлов из MinIO.

    :param s3: Инстанс S3Hook для работы с MinIO
    :param pg: Инстанс PostgresHook для работы с БД
    :param bucket: Название бакета MinIO
    :return: Список новых файлов для обработки
    """
    processed_files = get_processed_files(pg)
    files = s3.list_keys(bucket_name=bucket) or []
    return [f for f in files if f not in processed_files]


def process_file(file_key: str, s3: S3Hook, pg: PostgresHook, insert_sql: str, bucket: str ) -> Tuple[bool, str]:
    """
    Обрабатывает один файл: читает, валидирует, записывает в БД, отмечает как обработанный.

    :param file_key: Имя файла в MinIO
    :param s3: Инстанс S3Hook
    :param pg: Инстанс PostgresHook
    :param insert_sql: SQL-запрос для вставки события
    :param bucket: Название бакета MinIO
    :return: Кортеж (успешно ли обработан, сообщение с ошибкой или пустая строка)
    """
    try:
        content = s3.read_key(bucket_name=bucket, key=file_key)
        if isinstance(content, bytes):
            content = content.decode("utf-8")
        data = json.loads(content)
        validated = Event(**data)

        row = {
            'event_id': validated.event_id,
            'event_time': validated.event_time,
            'user_id': validated.user.user_id,
            'email': validated.user.email,
            'referral_code': validated.user.referral_code,
            'user_name': validated.user.profile.name,
            'birth_date': validated.user.profile.birth_date,
            'profile_created_at': validated.user.profile.created_at,
            'product_id': validated.product.product_id,
            'product_name': validated.product.name,
            'category': validated.product.category,
            'supplier': validated.product.supplier,
            'price': validated.product.price,
            'session_id': validated.session.session_id,
            'session_start_time': validated.session.start_time,
            'session_end_time': validated.session.end_time,
            'device_type': validated.session.device.type,
            'device_os': validated.session.device.os,
            'location_country': validated.session.device.location.country,
            'location_city': validated.session.device.location.city,
            'order_id': validated.order.order_id,
            'payment_id': validated.order.payment_id,
            'order_items': validated.order.order_items,
            'total_amount': validated.order.total_amount,
            'order_status': validated.order.status,
            'campaign': validated.marketing.campaign or '',
            'promocode': validated.marketing.promocode or '',
            'user_campaign_id': validated.marketing.user_campaign_id,
            'raw_payload': json.dumps(data),
        }

        pg.run(insert_sql, parameters=row)
        mark_file_as_processed(pg, file_key)
        return True, ""

    except Exception as e:
        logging.exception(f"Ошибка при обработке {file_key}")
        return False, f"{file_key}: {e}"
    

if __name__ == "__main__":
    pass