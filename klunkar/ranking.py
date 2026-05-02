from datetime import date
from typing import Any

import psycopg

from klunkar import config, db
from klunkar.models import (
    MunskankarnaPayload,
    RankedWine,
    Source,
    VivinoPayload,
    Wine,
)

_VALUE_RATING_ORDER = {
    "fynd": 3,
    "mer än prisvärt": 2,
    "prisvärt": 1,
    "ej prisvärt": 0,
}


def _bayesian(r: float, v: int, c: float, m: int) -> float:
    return (v / (v + m)) * r + (m / (v + m)) * c


def _vivino_global_mean(rows: list[tuple[Wine, dict[str, dict[str, Any]]]]) -> float:
    ratings = [
        p[Source.VIVINO]["ratings_average"]
        for _, p in rows
        if Source.VIVINO in p and "ratings_average" in p[Source.VIVINO]
    ]
    return sum(ratings) / len(ratings) if ratings else 0.0


def _score_for(
    source: Source,
    payload: dict[str, Any],
    wine: Wine,
    *,
    vivino_global_mean: float,
) -> tuple[float, tuple[Any, ...]]:
    """Return (rank_score, tiebreak_key) for a wine's payload from a given source."""
    if source is Source.VIVINO:
        score = _bayesian(
            payload["ratings_average"],
            payload["ratings_count"],
            vivino_global_mean,
            config.VIVINO_RATING_PRIOR,
        )
        tiebreak = (-payload["ratings_count"], wine.price or 0.0)
        return score, tiebreak
    if source is Source.MUNSKANKARNA:
        score = float(payload["score"])
        value_rank = _VALUE_RATING_ORDER.get(payload.get("value_rating") or "", -1)
        tiebreak = (-value_rank, wine.price or 0.0)
        return score, tiebreak
    raise ValueError(f"unknown source: {source}")


def build_ranked_view(
    conn: psycopg.Connection,
    release_date: date,
    *,
    source: Source | str,
    value_ratings: set[str] | None = None,
    wine_types: set[str] | None = None,
) -> list[RankedWine]:
    source = Source(source)  # accept string at the boundary (CLI, DB)
    rows = db.get_wines_with_enrichments(conn, release_date)
    vivino_global_mean = _vivino_global_mean(rows) if source is Source.VIVINO else 0.0

    scored: list[tuple[float, tuple[Any, ...], RankedWine]] = []
    for wine, payloads in rows:
        if source not in payloads:
            continue
        if wine_types and (wine.wine_type or "") not in wine_types:
            continue
        if value_ratings:
            mp = payloads.get(Source.MUNSKANKARNA, {})
            if mp.get("value_rating") not in value_ratings:
                continue

        rank_score, tiebreak = _score_for(
            source,
            payloads[source],
            wine,
            vivino_global_mean=vivino_global_mean,
        )
        scored.append(
            (
                rank_score,
                tiebreak,
                RankedWine(
                    wine=wine,
                    rank_score=rank_score,
                    vivino=(
                        VivinoPayload(**payloads[Source.VIVINO])
                        if Source.VIVINO in payloads
                        else None
                    ),
                    munskankarna=(
                        MunskankarnaPayload(**payloads[Source.MUNSKANKARNA])
                        if Source.MUNSKANKARNA in payloads
                        else None
                    ),
                ),
            )
        )

    scored.sort(key=lambda t: (-t[0], t[1]))
    return [r for _, _, r in scored]
