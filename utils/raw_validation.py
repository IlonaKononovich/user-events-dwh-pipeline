from typing import Literal
from pydantic import BaseModel, EmailStr, Field, validator
from datetime import datetime, date
from uuid import UUID
from decimal import Decimal
from typing import Optional

# --- Модели вложенных структур события ---


class Profile(BaseModel):
    """
    Данные профиля пользователя
    """
    name: str
    birth_date: date  # формат YYYY-MM-DD
    created_at: datetime  # дата регистрации профиля


class User(BaseModel):
    """
    Информация о пользователе
    """
    user_id: UUID
    email: EmailStr
    referral_code: str
    profile: Profile


class Product(BaseModel):
    """
    Данные о продукте (если есть в событии)
    """
    product_id: UUID
    name: str
    category: str
    supplier: str
    price: Decimal  # точный тип для хранения денежных значений


class Location(BaseModel):
    """
    Геолокация устройства
    """
    country: str
    city: str


class Device(BaseModel):
    """
    Устройство, с которого произошло событие
    """
    type: Literal["mobile", "desktop", "tablet"]
    os: Literal["Windows", "iOS", "Linux", "Android"]
    location: Location


class Session(BaseModel):
    """
    Сессия пользователя
    """
    session_id: UUID
    start_time: datetime
    end_time: datetime
    device: Device

    @validator("end_time")
    def check_end_after_start(cls, v, values):
        """
        Валидация: окончание сессии не раньше её начала
        """
        start_time = values.get("start_time")
        if start_time and v < start_time:
            raise ValueError("end_time должен быть позже или равен start_time")
        return v


class Order(BaseModel):
    """
    Заказ, связанный с событием
    """
    order_id: UUID
    payment_id: UUID
    order_items: int = Field(..., ge=1)  # хотя бы 1 товар
    total_amount: Decimal = Field(..., ge=0)  # неотрицательная сумма
    status: Literal["created", "paid", "shipped", "cancelled"]


class Marketing(BaseModel):
    """
    Маркетинговая информация, связанная с пользователем
    """
    campaign: Optional[str] = None
    promocode: Optional[str] = None
    user_campaign_id: Optional[UUID] = None


# --- Корневая модель события ---


class Event(BaseModel):
    """
    Основная модель события, поступающего в RAW слой.
    Используется для валидации входных данных перед загрузкой в БД.
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
        Проверка на пустые поля верхнего уровня
        """
        if not v:
            raise ValueError("Поле не может быть пустым")
        return v
