from typing import Optional, Literal
from pydantic import BaseModel, EmailStr, Field, validator
from datetime import datetime, date
from uuid import UUID


# Модели вложенных объектов
class Profile(BaseModel):
    name: str
    birth_date: date  # только дата, без времени
    created_at: datetime


class User(BaseModel):
    user_id: UUID
    email: EmailStr
    referral_code: Optional[str] = None
    profile: Profile


class Product(BaseModel):
    product_id: UUID
    name: str
    category: str
    supplier: str
    price: float


class Location(BaseModel):
    country: str
    city: str


class Device(BaseModel):
    type: Literal["mobile", "desktop", "tablet"]
    os: Literal["Windows", "iOS", "Linux", "Android"]
    location: Location


class Session(BaseModel):
    session_id: UUID
    start_time: datetime
    end_time: datetime
    device: Device

    @validator("end_time")
    def check_end_after_start(cls, v, values):
        start_time = values.get("start_time")
        if start_time and v < start_time:
            raise ValueError("end_time должен быть позже или равен start_time")
        return v


class Order(BaseModel):
    order_id: UUID
    payment_id: UUID
    order_items: int = Field(..., ge=1)
    total_amount: float = Field(..., ge=0)
    status: Literal["created", "paid", "shipped", "cancelled"]


class Marketing(BaseModel):
    campaign: Optional[str] = None
    promocode: Optional[str] = None
    user_campaign_id: Optional[UUID] = None


# Корневая модель события
class Event(BaseModel):
    event_id: UUID
    event_time: datetime
    user: User
    product: Product
    session: Session
    order: Order
    marketing: Marketing

    @validator("event_id", "event_time")
    def not_empty(cls, v):
        if not v:
            raise ValueError("Поле не может быть пустым")
        return v
