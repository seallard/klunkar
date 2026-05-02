import logging
import re
from datetime import date
from urllib.parse import urljoin

import httpx
import psycopg
from bs4 import BeautifulSoup

from klunkar.models import MunskankarnaPayload, Source, Wine
from klunkar.sources.base import EnrichmentResult

log = logging.getLogger(__name__)

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


class MunskankarnaEnricher:
    name = Source.MUNSKANKARNA
    display_name = "Munskänkarna"

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
