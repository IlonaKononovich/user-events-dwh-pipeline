"""
Модели Pydantic для валидации структуры событий, поступающих в RAW слой.

Определены вложенные модели для описания профиля пользователя, устройства, сессии,
продуктов, заказов и маркетинговых данных.

Корневая модель Event агрегирует все данные события и обеспечивает
валидацию обязательных полей и логическую проверку целостности данных
(например, обязательность продуктов для определённых типов событий).
"""

from typing import Literal, Optional, List
from pydantic import BaseModel, EmailStr, Field, validator
from datetime import datetime, date
from uuid import UUID


# Модели вложенных структур события

class Profile(BaseModel):
    """
    Данные профиля пользователя.

    :param name: Имя пользователя.
    :param birth_date: Дата рождения в формате YYYY-MM-DD.
    :param created_at: Дата и время создания профиля.
    """
    name: str
    birth_date: date
    created_at: datetime


class User(BaseModel):
    """
    Информация о пользователе.

    :param user_id: Уникальный идентификатор пользователя.
    :param email: Email пользователя, проверяется через EmailStr.
    :param referral_code: Реферальный код пользователя.
    :param profile: Профиль пользователя (вложенная модель Profile).
    """
    user_id: UUID
    email: EmailStr
    referral_code: str
    profile: Profile


class Product(BaseModel):
    """
    Данные о продукте.

    :param product_id: Уникальный идентификатор продукта.
    :param name: Название продукта.
    :param category: Категория продукта.
    :param supplier: Поставщик продукта.
    :param price: Цена продукта.
    :param quantity: Количество товара, минимум 1.
    """
    product_id: UUID
    name: str
    category: str
    supplier: str
    price: float
    quantity: int = Field(..., ge=1)


class Location(BaseModel):
    """
    Геолокация устройства.

    :param country: Страна устройства.
    :param city: Город устройства.
    """
    country: str
    city: str


class Device(BaseModel):
    """
    Устройство, с которого произошло событие.

    :param type: Тип устройства, ограничен значениями.
    :param os: Операционная система устройства, ограничена значениями.
    :param location: Геолокация устройства (вложенная модель Location).
    """
    type: Literal["mobile", "desktop", "tablet"]
    os: Literal["Windows", "iOS", "Linux", "Android"]
    location: Location


class Session(BaseModel):
    """
    Сессия пользователя.

    :param session_id: Уникальный идентификатор сессии.
    :param start_time: Время начала сессии.
    :param end_time: Время окончания сессии.
    :param pages_viewed: Количество просмотренных страниц (опционально).
    :param device: Устройство, с которого происходила сессия (вложенная модель Device).
    """
    session_id: UUID
    start_time: datetime
    end_time: datetime
    pages_viewed: Optional[int] = Field(None, ge=0)
    device: Device

    @validator("end_time")
    def check_end_after_start(cls, v, values):
        """
        Валидация: окончание сессии не может быть раньше её начала.

        :param v: Значение end_time.
        :param values: Другие поля модели, включая start_time.
        :return: Валидированное значение end_time.
        :raises ValueError: Если end_time раньше start_time.
        """
        start_time = values.get("start_time")
        if start_time and v < start_time:
            raise ValueError("end_time должен быть позже или равен start_time")
        return v


class Order(BaseModel):
    """
    Заказ, связанный с событием.

    :param order_id: Уникальный идентификатор заказа (опционально).
    :param payment_id: Уникальный идентификатор платежа (опционально).
    :param order_items: Количество товаров в заказе (опционально).
    :param total_amount: Общая сумма заказа (опционально).
    :param status: Статус заказа, ограничен строковыми значениями (опционально).
    """
    order_id: Optional[UUID] = None
    payment_id: Optional[UUID] = None
    order_items: Optional[int] = Field(None, ge=1)
    total_amount: Optional[float] = Field(None, ge=0)
    status: Optional[Literal["created", "paid", "shipped", "cancelled"]] = None


class Marketing(BaseModel):
    """
    Маркетинговая информация, связанная с пользователем.

    :param campaign: Название кампании (опционально).
    :param promocode: Промокод (опционально).
    :param user_campaign_id: Уникальный идентификатор кампании пользователя (опционально).
    """
    campaign: Optional[str] = None
    promocode: Optional[str] = None
    user_campaign_id: Optional[UUID] = None


# Корневая модель события

class Event(BaseModel):
    """
    Основная модель события, поступающего в RAW слой.
    Используется для валидации входных данных перед загрузкой в БД.

    :param event_id: Уникальный идентификатор события.
    :param event_type: Тип события (page_view, add_to_cart, purchase).
    :param event_time: Время события.
    :param event_date: Дата события.
    :param user: Информация о пользователе (вложенная модель User).
    :param session: Информация о сессии (вложенная модель Session).
    :param products: Список продуктов (для add_to_cart и purchase).
    :param order: Информация о заказе (опционально).
    :param marketing: Маркетинговая информация (опционально).
    :param raw_payload: Исходный JSON события.
    """
    event_id: UUID
    event_type: Literal["page_view", "add_to_cart", "purchase"]
    event_time: datetime
    event_date: date

    user: User
    session: Session
    products: Optional[List[Product]] = None
    order: Optional[Order] = None
    marketing: Optional[Marketing] = None

    raw_payload: Optional[dict] = None

    @validator("event_id", "event_time")
    def not_empty(cls, v):
        """
        Проверка на пустые поля верхнего уровня.

        :param v: Значение поля.
        :return: Проверенное значение поля.
        :raises ValueError: Если значение пустое.
        """
        if not v:
            raise ValueError("Поле не может быть пустым")
        return v

    @validator("products", always=True)
    def check_products_for_event(cls, v, values):
        """
        Проверяет, что для purchase и add_to_cart указан список продуктов.

        :param v: Список продуктов.
        :param values: Другие значения модели.
        :return: Список продуктов или None.
        :raises ValueError: Если для нужных событий список отсутствует.
        """
        if values.get("event_type") in ("purchase", "add_to_cart") and not v:
            raise ValueError("Для purchase/add_to_cart должен быть список продуктов")
        return v


if __name__ == "__main__":
    pass
