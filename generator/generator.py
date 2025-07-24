import os
import io
import json
import time
import logging
from uuid import uuid4
from random import random
from datetime import datetime, timedelta, timezone
from faker import Faker
from minio import Minio
from minio.error import S3Error
from utils.telegram_logger import notify_telegram

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s - %(message)s'
)

# белорусская/русская локализация
fake = Faker("ru_RU") 

# Фиксированные категории и товары для корректных метрик
CATEGORIES = [
    {"name": "Электроника", "products": ["Смартфон", "Ноутбук", "Наушники"]},
    {"name": "Одежда", "products": ["Футболка", "Джинсы", "Куртка"]},
    {"name": "Дом и кухня", "products": ["Чайник", "Пылесос", "Сковородка"]},
    {"name": "Детские товары", "products": ["Игрушка", "Коляска", "Подгузники"]}
]

# Конфигурация MinIO из переменных окружения
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_BUCKET = os.getenv("MINIO_BUCKET")

client = Minio(
    endpoint=MINIO_ENDPOINT.replace("http://", "").replace("https://", ""),
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=MINIO_ENDPOINT.startswith("https")
)

def ensure_bucket_exists():
    try:
        if not client.bucket_exists(MINIO_BUCKET):
            client.make_bucket(MINIO_BUCKET)
            logging.info(f"Создан бакет: {MINIO_BUCKET}")
        else:
            logging.info(f"Бакет {MINIO_BUCKET} уже существует")
    except S3Error as e:
        error_message = f"[x] Ошибка при создании бакета: {e}"
        logging.error(error_message)
        notify_telegram(error_message)
        raise


def generate_event():
    # Текущее время события в UTC — используется как базовая временная метка
    event_time = datetime.now(timezone.utc)
    event_time_str = event_time.isoformat()

    # Выбор категории и продукта из предопределённого списка
    category = fake.random_element(CATEGORIES)
    product_name = fake.random_element(category["products"])

    # Генерация цены товара и количества
    price = round(fake.pyfloat(min_value=10.0, max_value=1000.0), 2)
    quantity = fake.random_int(min=1, max=100)
    total_amount = round(price * quantity, 2)

    # Длительность сессии
    session_duration = timedelta(minutes=round(random() * 30 + 1))
    end_time = (event_time + session_duration).isoformat()

    return {
        # Уникальный идентификатор события
        "event_id": str(uuid4()),

        # Временная метка события
        "event_time": event_time_str,

        # Информация о пользователе и его профиле
        "user": {
            "user_id": str(uuid4()),
            "email": fake.email(),
            "referral_code": fake.uuid4(),  # Код приглашения
            "profile": {
                "name": fake.name(),
                "birth_date": fake.date_of_birth(minimum_age=18, maximum_age=65).isoformat(),
                "created_at": event_time_str  # Дата регистрации совпадает с датой события
            }
        },

        # Сведения о товаре и поставщике
        "product": {
            "product_id": str(uuid4()),
            "name": product_name,
            "category": category["name"],
            "supplier": fake.company(),
            "price": price
        },

        # Данные сессии пользователя
        "session": {
            "session_id": str(uuid4()),
            "start_time": event_time_str,
            "end_time": end_time,
            "device": {
                "type": fake.random_element(["mobile", "desktop", "tablet"]),
                "os": fake.random_element(["Windows", "iOS", "Linux", "Android"]),
                "location": {
                    "country": "Беларусь",
                    "city": fake.city()
                }
            }
        },

        # Заказ и платёжная информация
        "order": {
            "order_id": str(uuid4()),
            "payment_id": str(uuid4()),
            "order_items": quantity, 
            "total_amount": total_amount,
            "status": fake.random_element(["created", "paid", "shipped", "cancelled"])
        },

        # Информация о маркетинговом взаимодействии
        "marketing": {
            "campaign": fake.word(), 
            "promocode": fake.lexify(text="?????-2025"),  # Промокод в формате XXXX-2024
            "user_campaign_id": str(uuid4())
        }
    }


def main():
    # Проверка наличия бакета в MinIO (создаёт, если не существует)
    ensure_bucket_exists()

    while True:
        try:
            # Генерация события и подготовка имени файла по текущей дате/времени
            event = generate_event()
            timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
            filename = f"event_{timestamp}.json"

            # Преобразование в байты и сохранение в MinIO
            json_bytes = json.dumps(event, indent=2, ensure_ascii=False).encode("utf-8")
            client.put_object(
                bucket_name=MINIO_BUCKET,
                object_name=filename,
                data=io.BytesIO(json_bytes),
                length=len(json_bytes),
                content_type="application/json"
            )

            logging.info(f"Событие {filename} успешно загружено в MinIO")

        except Exception as e:
            error_message = f"[x] Ошибка генерации или загрузки события: {e}"
            logging.error(error_message)
            notify_telegram(error_message)

        # Задержка в 60 секунд перед генерацией следующего события
        time.sleep(60)


if __name__ == "__main__":
    main()

