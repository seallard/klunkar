from datetime import date
from typing import Any

import psycopg

from klunkar import db
from klunkar.models import BaseSourcePayload, RankedWine, Source
from klunkar.sources import ENRICHERS


def build_ranked_view(
    conn: psycopg.Connection,
    release_date: date,
    *,
    source: Source | str,
    value_ratings: set[str] | None = None,
    wine_types: set[str] | None = None,
    countries: set[str] | None = None,
) -> list[RankedWine]:
    source = Source(source)
    enricher = ENRICHERS[source]
    rows = db.get_wines_with_enrichments(conn, release_date)
    ctx = enricher.prepare_context(rows)

    scored: list[tuple[float, tuple[Any, ...], RankedWine]] = []
    for wine, raw_payloads in rows:
        if source not in raw_payloads:
            continue
        if wine_types and (wine.wine_type or "") not in wine_types:
            continue
        if countries and (wine.country or "") not in countries:
            continue
        if value_ratings:
            mp = raw_payloads.get(Source.MUNSKANKARNA, {})
            if mp.get("value_rating") not in value_ratings:
                continue

        typed_payloads: dict[Source, BaseSourcePayload] = {
            s: ENRICHERS[s].payload_model(**raw_payloads[s]) for s in ENRICHERS if s in raw_payloads
        }
        rank_score, tiebreak = enricher.score(typed_payloads[source], wine, ctx)
        scored.append(
            (
                rank_score,
                tiebreak,
                RankedWine(wine=wine, rank_score=rank_score, payloads=typed_payloads),
            )
        )

    scored.sort(key=lambda t: (-t[0], t[1]))
    return [r for _, _, r in scored]
