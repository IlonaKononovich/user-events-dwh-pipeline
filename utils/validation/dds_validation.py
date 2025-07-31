"""
Модели Pydantic для Data Vault и DWH-слоя.

Содержит описание сущностей измерений (dim) и фактов (fact),
используемых в DDS (Data Delivery Service) и Data Warehouse.

Каждая модель описывает структуру таблиц с валидацией полей,
обеспечивая корректность и целостность данных на уровне приложения.
"""

from pydantic import BaseModel, EmailStr, Field, validator
from uuid import UUID
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Literal, Optional, List, Any


class DimUser(BaseModel):
    """
    Модель для таблицы dds.dim_user.

    :param user_id: UUID пользователя (уникальный).
    :param email: Email пользователя.
    :param referral_code: Реферальный код.
    :param user_name: Имя пользователя.
    :param birth_date: Дата рождения.
    :param profile_created_at: Время создания профиля с таймзоной.
    """
    user_id: UUID
    email: EmailStr
    referral_code: str
    user_name: str
    birth_date: date
    profile_created_at: datetime


class DimProduct(BaseModel):
    """
    Модель для таблицы dds.dim_product.

    :param product_id: UUID продукта (уникальный).
    :param name: Название продукта.
    :param category: Категория продукта.
    :param supplier: Поставщик.
    :param price: Цена продукта (>= 0).
    """
    product_id: int
    name: str
    category: str
    supplier: str
    price: Decimal = Field(..., ge=0)


class DimDevice(BaseModel):
    """
    Модель для dds.dim_device с ограничениями на значения.

    :param device_type: Тип устройства ('mobile', 'desktop', 'tablet').
    :param device_os: ОС устройства ('Windows', 'iOS', 'Linux', 'Android').
    """
    device_type: Literal['mobile', 'desktop', 'tablet']
    device_os: Literal['Windows', 'iOS', 'Linux', 'Android']


class DimLocation(BaseModel):
    """
    Модель для dds.dim_location.

    :param country: Страна.
    :param city: Город.
    """
    country: str
    city: str


class DimMarketing(BaseModel):
    """
    Модель для dds.dim_marketing (опциональные поля).

    :param campaign: Название кампании.
    :param promocode: Промокод.
    :param user_campaign_id: UUID кампании пользователя.
    """
    campaign: Optional[str] = None
    promocode: Optional[str] = None
    user_campaign_id: Optional[UUID] = None


class DimSession(BaseModel):
    """
    Модель для dds.dim_session.

    :param session_id: UUID сессии.
    :param user_id: UUID пользователя (будет преобразован к INT surrogate key).
    :param start_time: Время начала сессии с таймзоной.
    :param end_time: Время окончания сессии с таймзоной (>= start_time).
    :param device_id: INT device surrogate key.
    :param location_id: INT location surrogate key.
    """
    session_id: UUID
    user_id: UUID  # Валидация на UUID, преобразование в surrogate key будет на уровне ETL
    start_time: datetime
    end_time: datetime
    device_id: int
    location_id: int

    @validator('end_time')
    def check_end_after_start(cls, v, values):
        start = values.get('start_time')
        if start and v < start:
            raise ValueError('end_time не может быть раньше start_time')
        return v


class FactEvent(BaseModel):
    """
    Модель для dds.fact_event.

    :param event_id: UUID события.
    :param event_type: Тип события ('page_view', 'add_to_cart', 'purchase').
    :param event_time: Время события.
    :param event_date: Дата события.
    :param user_id: UUID  # surrogate key dim_user (UUID)
    :param session_id: INT surrogate key из dim_session.
    :param device_id: INT surrogate key из dim_device.
    :param location_id: INT surrogate key из dim_location.
    :param date_id: INT surrogate key из dim_date.
    :param marketing_id: INT surrogate key из dim_marketing (опционально).
    :param pages_viewed: Кол-во просмотренных страниц.
    :param products: JSONB с товарами.
    :param order_id: UUID заказа (nullable).
    :param payment_id: UUID платежа (nullable).
    :param order_items: Кол-во товаров в заказе (nullable).
    :param total_amount: Общая сумма (nullable).
    :param order_status: Статус заказа ('created', 'paid', 'shipped', 'cancelled') (nullable).
    :param raw_payload: Исходный JSON события.
    """
    event_id: UUID
    event_type: Literal['page_view', 'add_to_cart', 'purchase']
    event_time: datetime
    event_date: date

    user_id: UUID
    session_id: int
    device_id: int
    location_id: int
    date_id: int
    marketing_id: Optional[int] = None

    pages_viewed: Optional[int] = None
    products: Optional[Any] = None

    order_id: Optional[UUID] = None
    payment_id: Optional[UUID] = None
    order_items: Optional[int] = None
    total_amount: Optional[Decimal] = None
    order_status: Optional[Literal['created', 'paid', 'shipped', 'cancelled']] = None

    raw_payload: Any

    @validator('event_time')
    def check_event_time(cls, v):
        if v.tzinfo is None:
            raise ValueError('event_time должен содержать timezone')
        return v


class FactOrder(BaseModel):
    """
    Модель для dds.fact_order.

    :param order_id: UUID заказа.
    :param payment_id: UUID платежа.
    :param order_items: Кол-во товаров (>=1).
    :param total_amount: Сумма заказа (>=0).
    :param order_status: Статус ('created', 'paid', 'shipped', 'cancelled').
    :param user_id: INT surrogate key dim_user.
    :param product_id: INT surrogate key dim_product.
    :param session_id: INT surrogate key dim_session.
    :param device_id: INT surrogate key dim_device.
    :param location_id: INT surrogate key dim_location.
    :param date_id: INT surrogate key dim_date.
    :param marketing_id: INT surrogate key dim_marketing (опционально).
    :param event_time: Время события (<= now).
    :param raw_event_id: UUID исходного события.
    """
    order_id: Optional[UUID] = None
    payment_id: Optional[UUID] = None
    order_items: Optional[int] = Field(None, ge=1)
    total_amount: Optional[Decimal] = Field(None, ge=0)
    order_status: Optional[Literal['created', 'paid', 'shipped', 'cancelled']] = None

    user_id: UUID
    product_id: UUID
    session_id: int
    device_id: int
    location_id: int
    date_id: int
    marketing_id: Optional[int] = None

    event_time: datetime
    raw_event_id: Optional[UUID] = None

    @validator('event_time')
    def check_event_time_not_future(cls, v):
        now_utc = datetime.now(timezone.utc)
        if v.tzinfo is None:
            v = v.replace(tzinfo=timezone.utc)
        if v > now_utc:
            raise ValueError('event_time не может быть в будущем')
        return v


class FactSession(BaseModel):
    """
    Модель для dds.fact_session.

    :param session_dim_id: INT surrogate key dim_session.
    :param date_id: INT surrogate key dim_date.
    :param session_start_time: Время начала сессии.
    :param session_end_time: Время окончания сессии (>= start).
    :param pages_viewed: Кол-во просмотренных страниц.
    :param events_count: Кол-во событий в сессии.
    :param is_converted: Флаг конверсии.
    """
    session_dim_id: int
    date_id: int
    session_start_time: datetime
    session_end_time: datetime
    pages_viewed: Optional[int] = None
    events_count: int = 0
    is_converted: bool = False

    @validator('session_end_time')
    def check_end_after_start(cls, v, values):
        start = values.get('session_start_time')
        if start and v < start:
            raise ValueError('session_end_time не может быть раньше session_start_time')
        return v


if __name__ == "__main__":
    pass
