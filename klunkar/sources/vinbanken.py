import logging
import re
from datetime import date
from typing import Any
from urllib.parse import urljoin

import httpx
import psycopg
from bs4 import BeautifulSoup, Tag

from klunkar.markdown import escape
from klunkar.models import BaseSourcePayload, Source, Wine
from klunkar.sources.base import Enricher, EnrichmentResult

log = logging.getLogger(__name__)


class VinbankenPayload(BaseSourcePayload):
    score: int
    fynd: bool = False
    tasting_note: str | None = None
    review_url: str | None = None


_BASE = "https://vinbanken.se"
_HUB_URL = _BASE + "/kategorier/nyheter-systembolaget/tillfalligt-sortiment-systembolaget"

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

_SB_NUMBER_RE = re.compile(r"#(\d{4,7})")
_SCORE_TOOLTIP_RE = re.compile(r"^(\d{1,3})/\d+$")
_FYND_RE = re.compile(r"^Fynd\s+\d{4}$", re.IGNORECASE)
_ARTICLE_ID_RE = re.compile(r"-(\d+)$")


def _date_needle(d: date) -> str:
    return f"-{d.day}-{_MONTHS_SV[d.month - 1]}-"


def _discover_article_urls(html: str, release_date: date) -> list[str]:
    needle = _date_needle(release_date)
    soup = BeautifulSoup(html, "html.parser")
    seen: set[str] = set()
    out: list[str] = []
    for a in soup.select("a[href]"):
        href = a.get("href", "")
        if "/artiklar/" not in href:
            continue
        if "tillfalligt-sortiment" not in href:
            continue
        if needle not in href:
            continue
        if not _ARTICLE_ID_RE.search(href):
            continue
        full = urljoin(_HUB_URL, href)
        if full in seen:
            continue
        seen.add(full)
        out.append(full)
    return out


def _parse_card(card: Tag) -> VinbankenPayload | None:
    meta = card.select_one(".meta")
    sb_match = _SB_NUMBER_RE.search(meta.get_text(" ", strip=True)) if meta else None
    if not sb_match:
        return None

    score: int | None = None
    for el in card.select("[data-tooltip]"):
        m = _SCORE_TOOLTIP_RE.match(el.get("data-tooltip", "").strip())
        if m:
            score = int(m.group(1))
            break
    if score is None:
        return None

    fynd = any(
        _FYND_RE.match(el.get("data-tooltip", "").strip()) for el in card.select("[data-tooltip]")
    )

    note_el = card.select_one(".prose-sm p")
    tasting_note = note_el.get_text(" ", strip=True) if note_el else None

    return VinbankenPayload(
        score=score,
        fynd=fynd,
        tasting_note=tasting_note,
    )


def _wine_anchor_url(page_url: str, sb_number: str) -> str:
    """Browser text-fragment URL that scrolls to the wine's SB-number text.

    `#:~:text=` is a standard browser feature (supported in current
    Chrome/Safari/Firefox); the `#` in `#90378` is URL-encoded as `%23`
    so it doesn't terminate the fragment.
    """
    return f"{page_url}#:~:text=%23{sb_number}"


def _parse_article(html: str, page_url: str) -> dict[str, VinbankenPayload]:
    soup = BeautifulSoup(html, "html.parser")
    out: dict[str, VinbankenPayload] = {}
    for card in soup.select("article.rounded-card"):
        meta = card.select_one(".meta")
        if not meta:
            continue
        sb_match = _SB_NUMBER_RE.search(meta.get_text(" ", strip=True))
        if not sb_match:
            continue
        payload = _parse_card(card)
        if payload is None:
            continue
        sb_number = sb_match.group(1)
        out[sb_number] = payload.model_copy(
            update={"review_url": _wine_anchor_url(page_url, sb_number)}
        )
    return out


class VinbankenEnricher(Enricher):
    name = Source.VINBANKEN
    display_name = "Vinbanken"
    payload_model = VinbankenPayload

    def enrich_release(
        self,
        release_date: date,
        wines: list[Wine],
        client: httpx.Client,
        conn: psycopg.Connection,
    ) -> list[EnrichmentResult]:
        try:
            hub = client.get(_HUB_URL, follow_redirects=True, timeout=20)
        except httpx.RequestError as e:
            log.warning("Vinbanken hub fetch failed: %s", e)
            return []
        if hub.status_code != 200:
            log.warning("Vinbanken hub returned %d", hub.status_code)
            return []

        article_urls = _discover_article_urls(hub.text, release_date)
        if not article_urls:
            log.info("Vinbanken: no articles found for %s on hub", release_date)
            return []

        merged: dict[str, VinbankenPayload] = {}
        for url in article_urls:
            try:
                r = client.get(url, follow_redirects=True, timeout=20)
            except httpx.RequestError as e:
                log.warning("Vinbanken article fetch failed for %s: %s", url, e)
                continue
            if r.status_code != 200:
                log.warning("Vinbanken article %s returned %d", url, r.status_code)
                continue
            try:
                parsed = _parse_article(r.text, url)
            except Exception:
                log.exception("Vinbanken parse failed for %s; excerpt=%r", url, r.text[:200])
                continue
            merged.update(parsed)

        log.info(
            "Vinbanken parsed %d wines across %d article(s) for %s; matching against %d SB wines",
            len(merged),
            len(article_urls),
            release_date,
            len(wines),
        )

        results: list[EnrichmentResult] = []
        for w in wines:
            payload = merged.get(w.sb_product_number)
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
        payload: VinbankenPayload,
        wine: Wine,
        ctx: Any,
    ) -> tuple[float, tuple[Any, ...]]:
        tiebreak = (-int(payload.fynd), wine.price or 0.0)
        return float(payload.score), tiebreak

    def render_row(self, payload: VinbankenPayload) -> str:
        chunk = f"Vinbanken: {payload.score}/100"
        if payload.fynd:
            chunk += " (fynd)"
        label = escape(chunk)
        return f"[{label}]({payload.review_url})" if payload.review_url else label
