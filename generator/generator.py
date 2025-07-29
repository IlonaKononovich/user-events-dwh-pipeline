import os
import io
import json
import time
import logging
from uuid import uuid4
from random import random, randint, choice
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

# Категории товаров с реальными диапазонами цен и количеством в заказе
CATEGORIES = [
    {"name": "Электроника", "products": [
        {"name": "Смартфон", "price_range": (800, 2500), "qty_range": (1, 2)},
        {"name": "Ноутбук", "price_range": (2000, 5000), "qty_range": (1, 1)},
        {"name": "Наушники", "price_range": (50, 300), "qty_range": (1, 3)}
    ]},
    {"name": "Одежда", "products": [
        {"name": "Футболка", "price_range": (30, 80), "qty_range": (1, 3)},
        {"name": "Джинсы", "price_range": (100, 200), "qty_range": (1, 2)},
        {"name": "Куртка", "price_range": (200, 600), "qty_range": (1, 1)}
    ]},
    {"name": "Дом и кухня", "products": [
        {"name": "Чайник", "price_range": (50, 150), "qty_range": (1, 1)},
        {"name": "Пылесос", "price_range": (200, 700), "qty_range": (1, 1)},
        {"name": "Сковородка", "price_range": (20, 80), "qty_range": (1, 2)}
    ]},
    {"name": "Детские товары", "products": [
        {"name": "Игрушка", "price_range": (10, 50), "qty_range": (1, 5)},
        {"name": "Коляска", "price_range": (300, 1000), "qty_range": (1, 1)},
        {"name": "Подгузники", "price_range": (30, 100), "qty_range": (1, 3)}
    ]}
]

# Реалистичные маркетинговые кампании
CAMPAIGNS = [
    "Твоя распродажа", "Большие скидки", "Черная пятница", "Супервыгода",
    "Время закупаться", "Любить себя", "День лучших покупок", "Cyber Monday"
]

# Пулы пользователей и сессий для реалистичности (повторяемость)
USER_POOL = [str(uuid4()) for _ in range(1000)]
SESSION_POOL = {}

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
    # Текущее время события в UTC — базовая временная метка
    event_time = datetime.now(timezone.utc)
    event_time_str = event_time.isoformat()

    # Выбираем пользователя (80% - из пула, 20% - новый)
    if random() < 0.8 and USER_POOL:
        user_id = choice(USER_POOL)
    else:
        user_id = str(uuid4())
        USER_POOL.append(user_id)

    # Определяем сессию (70% повторяемая, 30% новая)
    if user_id in SESSION_POOL and random() < 0.7:
        session_id = SESSION_POOL[user_id]
    else:
        session_id = str(uuid4())
        SESSION_POOL[user_id] = session_id

    # Выбираем категорию и товар, рассчитываем количество и цену
    category = choice(CATEGORIES)
    product = choice(category["products"])
    quantity = randint(*product["qty_range"])
    price = round(random() * (product["price_range"][1] - product["price_range"][0]) + product["price_range"][0], 2)
    total_amount = round(price * quantity, 2)

    # Длительность сессии — до 30 минут
    session_duration = timedelta(minutes=randint(1, 30))
    end_time = (event_time + session_duration).isoformat()

    # Маркетинг — 30% заказов с промокодом и кампанией, остальные — None
    if random() < 0.3:
        campaign = choice(CAMPAIGNS)
        promocode = fake.lexify(text="?????-2025")
        user_campaign_id = str(uuid4())
    else:
        campaign = None
        promocode = None
        user_campaign_id = None

    return {
        "event_id": str(uuid4()),
        "event_time": event_time_str,

        "user": {
            "user_id": user_id,
            "email": fake.email(),
            "referral_code": str(uuid4()),
            "profile": {
                "name": fake.name(),
                "birth_date": fake.date_of_birth(minimum_age=18, maximum_age=65).isoformat(),
                "created_at": event_time_str
            }
        },

        "product": {
            "product_id": str(uuid4()),
            "name": product["name"],
            "category": category["name"],
            "supplier": fake.company(),
            "price": price
        },

        "session": {
            "session_id": session_id,
            "start_time": event_time_str,
            "end_time": end_time,
            "device": {
                "type": choice(["mobile", "desktop", "tablet"]),
                "os": choice(["Windows", "iOS", "Linux", "Android"]),
                "location": {
                    "country": "Беларусь",
                    "city": fake.city()
                }
            }
        },

        "order": {
            "order_id": str(uuid4()),
            "payment_id": str(uuid4()),
            "order_items": quantity,
            "total_amount": total_amount,
            "status": choice(["created", "paid", "shipped", "cancelled"])
        },

        "marketing": {
            "campaign": campaign,
            "promocode": promocode,
            "user_campaign_id": user_campaign_id
        }
    }


def main():
    # Проверка и создание бакета в MinIO, если нужно
    ensure_bucket_exists()

    while True:
        try:
            # Генерация события и формирование имени файла с датой и временем
            event = generate_event()
            now = datetime.now(timezone.utc)
            timestamp = now.strftime("%Y%m%d_%H%M%S")
            date_prefix = now.strftime("%Y-%m-%d")
            filename = f"{date_prefix}/event_{timestamp}.json"

            # Сериализация события в JSON и отправка в MinIO
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
