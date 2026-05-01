from datetime import date

import psycopg

from klunkar import config, db
from klunkar.models import MunskankarnaPayload, RankedWine, VivinoPayload, Wine

_VALUE_RATING_ORDER = {
    "fynd": 3,
    "mer än prisvärt": 2,
    "prisvärt": 1,
    "ej prisvärt": 0,
}


def _bayesian(r: float, v: int, c: float, m: int) -> float:
    return (v / (v + m)) * r + (m / (v + m)) * c


def _vivino_global_mean(rows: list[tuple[Wine, dict[str, dict]]]) -> float:
    ratings = [
        p["vivino"]["ratings_average"]
        for _, p in rows
        if "vivino" in p and "ratings_average" in p["vivino"]
    ]
    return sum(ratings) / len(ratings) if ratings else 0.0


def build_ranked_view(
    conn: psycopg.Connection,
    release_date: date,
    *,
    source: str,
    value_ratings: set[str] | None = None,
) -> list[RankedWine]:
    rows = db.get_wines_with_enrichments(conn, release_date)

    if source == "vivino":
        global_mean = _vivino_global_mean(rows)
        prior_count = config.VIVINO_RATING_PRIOR

    out: list[tuple[float, tuple, RankedWine]] = []
    for wine, payloads in rows:
        if source not in payloads:
            continue
        if value_ratings:
            mp = payloads.get("munskankarna") or {}
            if mp.get("value_rating") not in value_ratings:
                continue
        vivino = VivinoPayload(**payloads["vivino"]) if "vivino" in payloads else None
        munskankarna = (
            MunskankarnaPayload(**payloads["munskankarna"])
            if "munskankarna" in payloads
            else None
        )

        if source == "vivino":
            assert vivino is not None
            score = _bayesian(
                vivino.ratings_average, vivino.ratings_count, global_mean, prior_count
            )
            tiebreak = (-vivino.ratings_count, wine.price or 0.0)
        elif source == "munskankarna":
            assert munskankarna is not None
            score = munskankarna.score
            value_rank = _VALUE_RATING_ORDER.get(munskankarna.value_rating or "", -1)
            tiebreak = (-value_rank, wine.price or 0.0)
        else:
            raise ValueError(f"unknown source: {source}")

        out.append((score, tiebreak, RankedWine(
            wine=wine, rank_score=score, vivino=vivino, munskankarna=munskankarna,
        )))

    out.sort(key=lambda t: (-t[0], t[1]))
    return [r for _, _, r in out]
