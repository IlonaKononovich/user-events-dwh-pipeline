from typing import Literal
from pydantic import BaseModel, EmailStr, Field, validator
from datetime import datetime, date
from uuid import UUID
from decimal import Decimal
from typing import Optional

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
    Данные о продукте

    :param product_id: Уникальный идентификатор продукта.
    :param name: Название продукта.
    :param category: Категория продукта.
    :param supplier: Поставщик продукта.
    :param price: Цена продукта с точным денежным типом Decimal.
    """
    product_id: UUID
    name: str
    category: str
    supplier: str
    price: Decimal


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
    :param device: Устройство, с которого происходила сессия (вложенная модель Device).
    """
    session_id: UUID
    start_time: datetime
    end_time: datetime
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

    :param order_id: Уникальный идентификатор заказа.
    :param payment_id: Уникальный идентификатор платежа.
    :param order_items: Количество товаров в заказе, минимум 1.
    :param total_amount: Общая сумма заказа, неотрицательная.
    :param status: Статус заказа, ограничен строковыми значениями.
    """
    order_id: UUID
    payment_id: UUID
    order_items: int = Field(..., ge=1)
    total_amount: Decimal = Field(..., ge=0)
    status: Literal["created", "paid", "shipped", "cancelled"]


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
    :param event_time: Время события.
    :param user: Информация о пользователе (вложенная модель User).
    :param product: Информация о продукте (вложенная модель Product).
    :param session: Информация о сессии (вложенная модель Session).
    :param order: Информация о заказе (вложенная модель Order).
    :param marketing: Маркетинговая информация (вложенная модель Marketing).
    """
    event_id: UUID
    event_time: datetime
    user: User
    product: Product
    session: Session
    order: Order
    marketing: Marketing

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


if __name__ == "__main__":
    pass
