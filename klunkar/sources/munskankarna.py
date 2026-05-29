import logging
import re
from datetime import date
from typing import Any
from urllib.parse import urljoin

import httpx
import psycopg
from bs4 import BeautifulSoup

from klunkar.markdown import escape
from klunkar.models import BaseSourcePayload, Source, Wine
from klunkar.sources.base import Enricher, EnrichmentResult

log = logging.getLogger(__name__)


class MunskankarnaPayload(BaseSourcePayload):
    score: float
    value_rating: str | None = None
    tasting_note: str | None = None
    review_url: str | None = None


_VALUE_RATING_ORDER = {
    "fynd": 3,
    "mer än prisvärt": 2,
    "prisvärt": 1,
    "ej prisvärt": 0,
}

_BASE = "https://www.munskankarna.se"
_RELEASE_URL = _BASE + "/sv/vinlocus/tillfalligt-sortiment-{slug}"

_MONTHS_SV = [
    "januari",
    "februari",
    "mars",
    "april",
    "maj",
    "juni",
    "juli",
    "augusti",
    "september",
    "oktober",
    "november",
    "december",
]

_VALUE_RATINGS = {"fynd", "mer än prisvärt", "prisvärt", "ej prisvärt"}

_SB_HREF_RE = re.compile(r"systembolaget\.se/(\d{3,7})")
_SCORE_RE = re.compile(r"^\s*(\d{1,2}(?:[.,]\d)?)\s*$")


def _release_slug(d: date) -> str:
    return f"{d.day}-{_MONTHS_SV[d.month - 1]}-{d.year}"


def release_url(release_date: date) -> str:
    return _RELEASE_URL.format(slug=_release_slug(release_date))


def _parse_score(raw: str) -> float | None:
    m = _SCORE_RE.match(raw)
    if not m:
        return None
    return float(m.group(1).replace(",", "."))


def _parse(html: str, page_url: str) -> dict[str, MunskankarnaPayload]:
    soup = BeautifulSoup(html, "html.parser")
    out: dict[str, MunskankarnaPayload] = {}
    for li in soup.select("li.groupedlist"):
        sb_link = li.find("a", href=_SB_HREF_RE)
        if not sb_link:
            continue
        m = _SB_HREF_RE.search(sb_link.get("href", ""))
        if not m:
            continue
        sb_number = m.group(1)
        if len(sb_number) == 7:
            sb_number = sb_number[:5]

        score_el = li.select_one(".wine-points")
        score = _parse_score(score_el.get_text(strip=True)) if score_el else None
        if score is None:
            continue

        value_rating = None
        stat = li.select_one('.c-wine-info__stat[name="category"] span')
        if stat:
            text = stat.get_text(strip=True).lower()
            if text in _VALUE_RATINGS:
                value_rating = text

        note_el = li.select_one(".c-wine-info__text")
        tasting_note = note_el.get_text(" ", strip=True) if note_el else None

        review_url = None
        link = li.select_one(".c-wine-info__headings h3 a[href]")
        if link:
            review_url = urljoin(page_url, link["href"])

        out[sb_number] = MunskankarnaPayload(
            score=score,
            value_rating=value_rating,
            tasting_note=tasting_note,
            review_url=review_url,
        )
    return out


class MunskankarnaEnricher(Enricher):
    name = Source.MUNSKANKARNA
    display_name = "Munskänkarna"
    payload_model = MunskankarnaPayload

    def enrich_release(
        self,
        release_date: date,
        wines: list[Wine],
        client: httpx.Client,
        conn: psycopg.Connection,
    ) -> list[EnrichmentResult]:
        url = release_url(release_date)
        try:
            r = client.get(url, follow_redirects=True, timeout=20)
        except httpx.RequestError as e:
            log.warning("Munskänkarna fetch failed for %s: %s", url, e)
            return []
        if r.status_code == 404:
            log.info("Munskänkarna page not yet up for %s (%s)", release_date, url)
            return []
        if r.status_code != 200:
            log.warning("Munskänkarna returned %d for %s", r.status_code, url)
            return []

        try:
            rows_by_number = _parse(r.text, url)
        except Exception:
            log.exception("Munskänkarna parse failed for %s; excerpt=%r", url, r.text[:200])
            return []

        log.info(
            "Munskänkarna parsed %d wines from %s; matching against %d SB wines",
            len(rows_by_number),
            url,
            len(wines),
        )

        results: list[EnrichmentResult] = []
        for w in wines:
            payload = rows_by_number.get(w.sb_product_number)
            if payload is None:
                continue
            results.append(
                EnrichmentResult(
                    sb_product_number=w.sb_product_number,
                    confidence=1.0,
                    payload=payload.model_dump(),
                )
            )
        return results

    def score(
        self,
        payload: MunskankarnaPayload,
        wine: Wine,
        ctx: Any,
    ) -> tuple[float, tuple[Any, ...]]:
        value_rank = _VALUE_RATING_ORDER.get(payload.value_rating or "", -1)
        tiebreak = (-value_rank, wine.price or 0.0)
        return float(payload.score), tiebreak

    def render_row(self, payload: MunskankarnaPayload) -> str:
        chunk = f"Munskänkarna: {payload.score:g}/20"
        if payload.value_rating:
            chunk += f" ({payload.value_rating})"
        label = escape(chunk)
        return f"[{label}]({payload.review_url})" if payload.review_url else label
