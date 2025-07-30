from pydantic import BaseModel, EmailStr, Field, validator
from uuid import UUID
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Literal
from typing import Optional


class DimUser(BaseModel):
    """
    Модель пользователя для таблицы dim_user.

    :param user_id: Уникальный идентификатор пользователя.
    :param email: Email пользователя.
    :param referral_code: Реферальный код пользователя.
    :param user_name: Имя пользователя.
    :param birth_date: Дата рождения пользователя.
    :param profile_created_at: Дата и время создания профиля пользователя.
    """
    user_id: UUID
    email: EmailStr
    referral_code: str
    user_name: str
    birth_date: date
    profile_created_at: datetime


class DimProduct(BaseModel):
    """
    Модель продукта для таблицы dim_product.

    :param product_id: Уникальный идентификатор продукта.
    :param product_name: Название продукта.
    :param category: Категория продукта.
    :param supplier: Поставщик продукта.
    :param price: Цена продукта (неотрицательная).
    """
    product_id: UUID
    product_name: str
    category: str
    supplier: str
    price: Decimal = Field(..., ge=0) 


class DimDevice(BaseModel):
    """
    Модель устройства для таблицы dim_device.

    :param device_type: Тип устройства.
    :param device_os: Операционная система устройства.
    """
    device_type: Literal['mobile', 'desktop', 'tablet']
    device_os: Literal['Windows', 'iOS', 'Linux', 'Android']


class DimLocation(BaseModel):
    """
    Модель локации для таблицы dim_location.

    :param country: Страна.
    :param city: Город.
    """
    country: str
    city: str


class DimSession(BaseModel):
    """
    Модель сессии для таблицы dim_session.
    Включает ссылки на DimDevice и DimLocation.

    :param session_id: Уникальный идентификатор сессии.
    :param session_start_time: Дата и время начала сессии.
    :param session_end_time: Дата и время окончания сессии.
    :param device: Устройство, с которого выполнялась сессия.
    :param location: Локация устройства.
    """
    session_id: UUID
    session_start_time: datetime
    session_end_time: datetime
    device: DimDevice
    location: DimLocation

    @validator('session_end_time')
    def check_session_end_after_start(cls, v, values):
        """
        Проверка, что окончание сессии не раньше её начала.

        :param v: Время окончания сессии.
        :param values: Другие поля модели (используется session_start_time).
        :return: Валидированное время окончания сессии.
        :raises ValueError: Если session_end_time < session_start_time.
        """
        start = values.get('session_start_time')
        if start and v < start:
            raise ValueError('session_end_time не может быть раньше session_start_time')
        return v


class DimMarketing(BaseModel):
    """
    Модель маркетинговой информации для таблицы dim_marketing (опционально).

    :param user_campaign_id: Уникальный идентификатор маркетинговой кампании для пользователя.
    :param campaign: Название кампании.
    :param promocode: Промокод кампании.
    """
    user_campaign_id: UUID
    campaign: str
    promocode: str


class FactOrder(BaseModel):
    """
    Модель факта заказа для таблицы fact_order.

    :param event_id: Уникальный идентификатор события.
    :param event_time: Дата и время события.
    :param order_id: Уникальный идентификатор заказа.
    :param payment_id: Уникальный идентификатор платежа.
    :param order_items: Количество товаров в заказе (не меньше 1).
    :param total_amount: Общая сумма заказа (неотрицательная).
    :param order_status: Статус заказа.
    :param user_id: Внешний ключ на dim_user.
    :param product_id: Внешний ключ на dim_product.
    :param session_id: Внешний ключ на dim_session.
    :param campaign: Название маркетинговой кампании (опционально).
    :param promocode: Промокод кампании (опционально).
    :param user_campaign_id: Уникальный идентификатор кампании пользователя (опционально).
    """
    event_id: UUID
    event_time: datetime
    order_id: UUID
    payment_id: UUID
    order_items: int = Field(..., ge=1)
    total_amount: Decimal = Field(..., ge=0)
    order_status: Literal['created', 'paid', 'shipped', 'cancelled']

    user_id: UUID
    product_id: UUID
    session_id: UUID

    campaign: Optional[str] = None
    promocode: Optional[str] = None
    user_campaign_id: Optional[UUID] = None

    @validator('event_time')
    def check_event_time_not_future(cls, v):
        """
        Проверка, что время события не в будущем.

        :param v: Дата и время события.
        :return: Валидированное время события.
        :raises ValueError: Если событие в будущем.
        """
        now_utc = datetime.now(timezone.utc)
        if v.tzinfo is None:
            v = v.replace(tzinfo=timezone.utc)
        if v > now_utc:
            raise ValueError('event_time не может быть в будущем')
        return v


if __name__ == "__main__":
    pass