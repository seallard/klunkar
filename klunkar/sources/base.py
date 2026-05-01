from datetime import date
from typing import Any, Protocol, runtime_checkable

import httpx
import psycopg
from pydantic import BaseModel, ConfigDict

from klunkar.models import Source, Wine


class EnrichmentResult(BaseModel):
    model_config = ConfigDict(frozen=True)

    sb_product_number: str
    confidence: float
    payload: dict[str, Any]


@runtime_checkable
class Enricher(Protocol):
    name: Source
    display_name: str

    def enrich_release(
        self,
        release_date: date,
        wines: list[Wine],
        client: httpx.Client,
        conn: psycopg.Connection,
    ) -> list[EnrichmentResult]: ...
