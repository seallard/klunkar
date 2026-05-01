from datetime import date

from pydantic import BaseModel


class Wine(BaseModel):
    sb_product_number: str
    sb_product_id: str
    release_date: date
    name: str
    producer: str
    sb_url: str
    price: float | None = None
    wine_type: str | None = None


class VivinoPayload(BaseModel):
    wine_id: int
    matched_name: str
    ratings_average: float
    ratings_count: int
    wine_url: str


class MunskankarnaPayload(BaseModel):
    score: float
    value_rating: str | None = None
    tasting_note: str | None = None
    review_url: str | None = None


class RankedWine(BaseModel):
    wine: Wine
    rank_score: float
    vivino: VivinoPayload | None = None
    munskankarna: MunskankarnaPayload | None = None
