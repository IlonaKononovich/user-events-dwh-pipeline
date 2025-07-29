from pydantic import BaseModel, EmailStr, Field, validator
from uuid import UUID
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Literal
from typing import Optional


class DimUser(BaseModel):
    """
    Модель пользователя для таблицы dim_user
    """
    user_id: UUID
    email: EmailStr
    referral_code: str
    user_name: str
    birth_date: date
    profile_created_at: datetime


class DimProduct(BaseModel):
    """
    Модель продукта для таблицы dim_product
    """
    product_id: UUID
    product_name: str
    category: str
    supplier: str
    price: Decimal = Field(..., ge=0)  # цена не может быть отрицательной


class DimDevice(BaseModel):
    """
    Модель устройства для таблицы dim_device
    """
    device_type: Literal['mobile', 'desktop', 'tablet']
    device_os: Literal['Windows', 'iOS', 'Linux', 'Android']


class DimLocation(BaseModel):
    """
    Модель локации для таблицы dim_location
    """
    country: str
    city: str


class DimSession(BaseModel):
    """
    Модель сессии для таблицы dim_session
    Включает ссылки на DimDevice и DimLocation
    """
    session_id: UUID
    session_start_time: datetime
    session_end_time: datetime
    device: DimDevice
    location: DimLocation

    @validator('session_end_time')
    def check_session_end_after_start(cls, v, values):
        start = values.get('session_start_time')
        if start and v < start:
            raise ValueError('session_end_time не может быть раньше session_start_time')
        return v


class DimMarketing(BaseModel):
    """
    Модель маркетинговой информации для таблицы dim_marketing (если нужна)
    """
    user_campaign_id: UUID
    campaign: str
    promocode: str


class FactOrder(BaseModel):
    """
    Модель факта заказа для таблицы fact_order
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
        now_utc = datetime.now(timezone.utc)  # aware datetime в UTC
        if v.tzinfo is None:
            # если v naive, считаем его в UTC
            v = v.replace(tzinfo=timezone.utc)
        if v > now_utc:
            raise ValueError('event_time не может быть в будущем')
        return v
