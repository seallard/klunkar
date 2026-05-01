import logging
from datetime import date

import httpx
import psycopg

from klunkar import vivino as _v
from klunkar.models import VivinoPayload, Wine
from klunkar.sources.base import EnrichmentResult

log = logging.getLogger(__name__)


class VivinoEnricher:
    name = "vivino"
    display_name = "Vivino"

    def enrich_release(
        self,
        release_date: date,
        wines: list[Wine],
        client: httpx.Client,
        conn: psycopg.Connection,
    ) -> list[EnrichmentResult]:
        _v.prime_session(client)
        cache: dict[str, list | None] = {}
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
            results.append(EnrichmentResult(
                sb_product_number=w.sb_product_number,
                confidence=match.fuzz_score / 100.0,
                payload=payload.model_dump(),
            ))
        return results
