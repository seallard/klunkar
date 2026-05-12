import logging
from datetime import date
from typing import Any

import httpx
import psycopg

from klunkar import config
from klunkar import vivino as _v
from klunkar.markdown import escape
from klunkar.models import BaseSourcePayload, Source, Wine
from klunkar.sources.base import Enricher, EnrichmentResult

log = logging.getLogger(__name__)


class VivinoPayload(BaseSourcePayload):
    wine_id: int
    matched_name: str
    ratings_average: float
    ratings_count: int
    wine_url: str


def _bayesian(r: float, v: int, c: float, m: int) -> float:
    return (v / (v + m)) * r + (m / (v + m)) * c


class VivinoEnricher(Enricher):
    name = Source.VIVINO
    display_name = "Vivino"
    payload_model = VivinoPayload

    def enrich_release(
        self,
        release_date: date,
        wines: list[Wine],
        client: httpx.Client,
        conn: psycopg.Connection,
    ) -> list[EnrichmentResult]:
        _v.prime_session(client)
        cache: dict[str, list[dict] | None] = {}
        results: list[EnrichmentResult] = []
        for w in wines:
            match = _v.lookup(w.producer, w.name, client, cache)
            if match is None:
                continue
            payload = VivinoPayload(
                wine_id=match.wine_id,
                matched_name=match.name,
                ratings_average=match.ratings_average,
                ratings_count=match.ratings_count,
                wine_url=match.wine_url,
            )
            results.append(
                EnrichmentResult(
                    sb_product_number=w.sb_product_number,
                    confidence=match.fuzz_score / 100.0,
                    payload=payload.model_dump(),
                )
            )
        return results

    def prepare_context(self, rows: list[tuple[Wine, dict[str, dict[str, Any]]]]) -> float:
        ratings = [
            p[Source.VIVINO]["ratings_average"]
            for _, p in rows
            if Source.VIVINO in p and "ratings_average" in p[Source.VIVINO]
        ]
        return sum(ratings) / len(ratings) if ratings else 0.0

    def score(
        self,
        payload: VivinoPayload,
        wine: Wine,
        ctx: float,
    ) -> tuple[float, tuple[Any, ...]]:
        rank = _bayesian(
            payload.ratings_average,
            payload.ratings_count,
            ctx,
            config.VIVINO_RATING_PRIOR,
        )
        tiebreak = (-payload.ratings_count, wine.price or 0.0)
        return rank, tiebreak

    def render_row(self, payload: VivinoPayload) -> str:
        label = escape(f"Vivino: {payload.ratings_average:.1f} ★")
        return f"[{label}]({payload.wine_url})"
