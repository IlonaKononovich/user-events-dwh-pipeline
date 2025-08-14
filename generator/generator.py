"""
Генератор событий для RAW слоя.

Функции:
- Генерирует реалистичные JSON-события (page_view, add_to_cart, purchase) с рандомизированными данными.
- Управляет пулом пользователей и сессий для имитации повторных взаимодействий.
- Загружает сформированные события в MinIO с организацией по датам.
- Логирует процесс и отправляет уведомления в Telegram при ошибках.
"""

import os
import io
import json
import time
import logging
import copy
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

# Белорусская/русская локализация
fake = Faker("ru_RU")

# Категории товаров с диапазонами цен
CATEGORIES = [
    {
        "name": "Электроника",
        "suppliers": [
            "БелЭлектро", "Электросила", "Минский завод электроники", "АйТиБел", "ГлобалТек"
        ],
        "products": [
            {"name": "Смартфон", "price_range": (800, 2500), "qty_range": (1, 2)},
            {"name": "Ноутбук", "price_range": (2000, 5000), "qty_range": (1, 1)},
            {"name": "Наушники", "price_range": (50, 300), "qty_range": (1, 3)}
        ]
    },
    {
        "name": "Одежда",
        "suppliers": [
            "Белтекс", "Модный Дом", "Швейная фабрика Минск", "СтильПром", "ТекстильПлюс"
        ],
        "products": [
            {"name": "Футболка", "price_range": (30, 80), "qty_range": (1, 3)},
            {"name": "Джинсы", "price_range": (100, 200), "qty_range": (1, 2)},
            {"name": "Куртка", "price_range": (200, 600), "qty_range": (1, 1)}
        ]
    },
    {
        "name": "Дом и кухня",
        "suppliers": [
            "БелКухня", "МебельГарант", "ТехноДом", "КомфортПлюс", "КерамикаСтиль"
        ],
        "products": [
            {"name": "Чайник", "price_range": (50, 150), "qty_range": (1, 1)},
            {"name": "Пылесос", "price_range": (200, 700), "qty_range": (1, 1)},
            {"name": "Сковородка", "price_range": (20, 80), "qty_range": (1, 2)}
        ]
    },
    {
        "name": "Детские товары",
        "suppliers": [
            "ДетиБел", "ИгрушкиМир", "КоляскиПлюс", "МалышСтиль", "ПодгузникСервис"
        ],
        "products": [
            {"name": "Игрушка", "price_range": (10, 50), "qty_range": (1, 5)},
            {"name": "Коляска", "price_range": (300, 1000), "qty_range": (1, 1)},
            {"name": "Подгузники", "price_range": (30, 100), "qty_range": (1, 3)}
        ]
    }
]


# Маркетинговые кампании
CAMPAIGNS = [
    "Твоя распродажа", "Большие скидки", "Черная пятница", "Супервыгода",
    "Время закупаться", "Любить себя", "День лучших покупок", "Cyber Monday"
]

# Города Беларуси (топ-30 по численности населения)
BY_CITIES = [
    "Минск", "Гомель", "Могилёв", "Витебск", "Гродно", "Брест", "Бобруйск",
    "Барановичи", "Пинск", "Орша", "Мозырь", "Новополоцк", "Лида", "Солигорск",
    "Слуцк", "Кобрин", "Светлогорск", "Жлобин", "Речица", "Волковыск", "Полоцк",
    "Новогрудок", "Молодечно", "Жодино", "Берёза", "Сморгонь", "Горки", "Дзержинск",
    "Осиповичи", "Калинковичи"
]

# Пулы пользователей и сессий
USER_POOL = {}      # user_id -> дата регистрации
SESSION_POOL = {}   # user_id -> session_id

# Конфигурация MinIO
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

DAYS_BACK = 90

def ensure_bucket_exists() -> None:
    """
    Проверяет существование бакета и создает его при необходимости.
    :return -> None
    """
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

def get_or_create_user() -> tuple[str, str]:
    """
    Возвращает user_id и дату регистрации.
    80% — старые пользователи.
    :return -> (user_id: str, created_at: str[ISO8601])
    """
    if random() < 0.8 and USER_POOL:
        user_id = choice(list(USER_POOL.keys()))
        created_at = USER_POOL[user_id]
    else:
        user_id = str(uuid4())
        created_at = datetime.now(timezone.utc).isoformat()
        USER_POOL[user_id] = created_at
    return user_id, created_at

def get_or_create_session(user_id: str) -> str:
    """
    Возвращает session_id для пользователя (70% — старая сессия).
    :param user_id: str
    :return -> session_id: str
    """
    if user_id in SESSION_POOL and random() < 0.7:
        return SESSION_POOL[user_id]
    else:
        session_id = str(uuid4())
        SESSION_POOL[user_id] = session_id
        return session_id

def generate_event() -> dict:
    """
    Генерирует событие веб-аналитики для RAW слоя.
    Поддерживает page_view / add_to_cart / purchase.
    :return -> dict (JSON-совместимый словарь события)
    """
    event_time = datetime.now(timezone.utc) - timedelta(days=randint(0, DAYS_BACK),
                                                    hours=randint(0,23),
                                                    minutes=randint(0,59),
                                                    seconds=randint(0,59))
    event_date = event_time.date().isoformat()
    event_time_str = event_time.isoformat()

    # Тип события
    rnd = random()
    if rnd < 0.8:
        event_type = "page_view"
    elif rnd < 0.95:
        event_type = "add_to_cart"
    else:
        event_type = "purchase"

    # Пользователь и сессия
    user_id, user_created_at = get_or_create_user()
    session_id = get_or_create_session(user_id)

    # Базовые данные сессии
    session_duration = timedelta(minutes=randint(1, 30))
    end_time = (event_time + session_duration).isoformat()
    pages_viewed = randint(1, 10)

    # Формируем продукты для события
    products = []
    category = choice(CATEGORIES)
    product = choice(category["products"])
    quantity = randint(*product["qty_range"])
    price = round(random() * (product["price_range"][1] - product["price_range"][0]) + product["price_range"][0], 2)
    total_amount = price * quantity

    products.append({
        "product_id": str(uuid4()),
        "name": product["name"],
        "category": category["name"],
        "supplier": choice(category["suppliers"]),
        "price": price,
        "quantity": quantity
    })

    if event_type == "purchase" and random() < 0.3:
        for _ in range(randint(1, 2)):
            category = choice(CATEGORIES)
            product = choice(category["products"])
            quantity = randint(*product["qty_range"])
            price = round(random() * (product["price_range"][1] - product["price_range"][0]) + product["price_range"][0], 2)
            total_amount += price * quantity
            products.append({
                "product_id": str(uuid4()),
                "name": product["name"],
                "category": category["name"],
                "supplier": choice(category["suppliers"]),
                "price": price,
                "quantity": quantity
            })

    # Маркетинг
    if event_type == "purchase" and random() < 0.3:
        campaign = choice(CAMPAIGNS)
        promocode = fake.lexify(text="?????-2025")
        user_campaign_id = str(uuid4())
    else:
        campaign = None
        promocode = None
        user_campaign_id = None

    # Статус заказа
    order_status = None
    if event_type == "purchase":
        order_status = choice(
            ["paid"] * 6 + ["shipped"] * 2 + ["created"] * 1 + ["cancelled"] * 1
        )

    # Формируем финальное событие
    event = {
        "event_id": str(uuid4()),
        "event_type": event_type,
        "event_time": event_time_str,
        "event_date": event_date,
        "user": {
            "user_id": user_id,
            "email": fake.email(),
            "referral_code": str(uuid4()),
            "profile": {
                "name": fake.name(),
                "birth_date": fake.date_of_birth(minimum_age=18, maximum_age=65).isoformat(),
                "created_at": user_created_at
            }
        },
        "session": {
            "session_id": session_id,
            "start_time": event_time_str,
            "end_time": end_time,
            "pages_viewed": pages_viewed,
            "device": {
                "type": choice(["mobile", "desktop", "tablet"]),
                "os": choice(["Windows", "iOS", "Linux", "Android"]),
                "location": {
                    "country": "Беларусь",
                    "city": choice(BY_CITIES)
                }
            }
        },
        "products": products if event_type in ["add_to_cart", "purchase"] else None,
        "order": {
            "order_id": str(uuid4()),
            "payment_id": str(uuid4()),
            "order_items": sum(p["quantity"] for p in products),
            "total_amount": float(round(total_amount, 2)),
            "status": order_status
        } if event_type == "purchase" else None,
        "marketing": {
            "campaign": campaign,
            "promocode": promocode,
            "user_campaign_id": user_campaign_id
        }
    }
    event_copy = copy.deepcopy(event)
    event_copy.pop("raw_payload", None)
    event["raw_payload"] = event_copy
    return event

def main() -> None:
    """
    Основной цикл генерации событий и загрузки их в MinIO.
    :return -> None
    """
    ensure_bucket_exists()

    while True:
        try:
            event = generate_event()
            event_time = datetime.fromisoformat(event["event_time"])
            timestamp = event_time.strftime("%Y%m%d_%H%M%S")
            random_suffix = f"{randint(100000, 999999)}"         # 6-значное случайное число
            filename = f"{event['event_date']}/event_{timestamp}_{random_suffix}.json"

            json_bytes = json.dumps(event, indent=2, ensure_ascii=False).encode("utf-8")
            client.put_object(
                bucket_name=MINIO_BUCKET,
                object_name=filename,
                data=io.BytesIO(json_bytes),
                length=len(json_bytes),
                content_type="application/json"
            )

            logging.info(f"Событие {filename} ({event['event_type']}) успешно загружено в MinIO")

        except Exception as e:
            error_message = f"[x] Ошибка генерации или загрузки события: {e}"
            logging.error(error_message)
            notify_telegram(error_message)

        time.sleep(2)

if __name__ == "__main__":
    main()