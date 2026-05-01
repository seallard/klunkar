from datetime import date
from typing import Any, Protocol, runtime_checkable

import httpx
import psycopg
from pydantic import BaseModel

from klunkar.models import Wine


class EnrichmentResult(BaseModel):
    sb_product_number: str
    confidence: float
    payload: dict[str, Any]


@runtime_checkable
class Enricher(Protocol):
    name: str
    display_name: str

    def enrich_release(
        self,
        release_date: date,
        wines: list[Wine],
        client: httpx.Client,
        conn: psycopg.Connection,
    ) -> list[EnrichmentResult]: ...
