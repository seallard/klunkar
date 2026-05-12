from datetime import date
from enum import StrEnum

from pydantic import BaseModel, ConfigDict, Field


class Source(StrEnum):
    VIVINO = "vivino"
    MUNSKANKARNA = "munskankarna"
    VINBANKEN = "vinbanken"


class BaseSourcePayload(BaseModel):
    model_config = ConfigDict(frozen=True)


class Wine(BaseModel):
    model_config = ConfigDict(frozen=True)

    sb_product_number: str
    sb_product_id: str
    release_date: date
    name: str
    producer: str
    sb_url: str
    price: float | None = None
    wine_type: str | None = None
    country: str | None = None


class RankedWine(BaseModel):
    model_config = ConfigDict(frozen=True)

    wine: Wine
    rank_score: float
    payloads: dict[Source, BaseSourcePayload] = Field(default_factory=dict)


class Subscriber(BaseModel):
    model_config = ConfigDict(frozen=True)

    chat_id: int
    max_price: float | None = None
    rank_source: Source = Source.MUNSKANKARNA
    value_filter: list[str] | None = None
    wine_type_filter: list[str] | None = None
    country_filter: list[str] | None = None
