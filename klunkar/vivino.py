import logging
import re
import time
import unicodedata
from dataclasses import dataclass

import httpx
from rapidfuzz import process, fuzz

log = logging.getLogger(__name__)

_WINERY_URL = "https://www.vivino.com/api/wineries/{seo_name}/wines"
_WINE_PAGE_URL = "https://www.vivino.com/w/{wine_id}"
_USER_AGENTS = [
    (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/123.0.0.0 Safari/537.36"
    ),
    (
        "Mozilla/5.0 (X11; Linux x86_64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4) "
        "AppleWebKit/605.1.15 (KHTML, like Gecko) "
        "Version/17.4 Safari/605.1.15"
    ),
]
_REQUEST_DELAY = 1.0  # seconds between requests
_RETRY_DELAY = 10.0   # seconds to wait before retrying after a 403
_MAX_RETRIES = 2

_ua_index = 0


def _next_headers() -> dict:
    global _ua_index
    ua = _USER_AGENTS[_ua_index % len(_USER_AGENTS)]
    _ua_index += 1
    return {
        "User-Agent": ua,
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://www.vivino.com/",
    }

_STRIP_PREFIXES = re.compile(
    r"^(bodegas?|weingut|weinguter|domaines?|chateaux?|quinta|tenuta|fattoria|maison|cantina|caves?)\s+",
    re.IGNORECASE,
)
_STRIP_SUFFIXES = re.compile(
    r"\s+(winery|estate|estates|vineyard|vineyards|wines|viticultore|viticultori|wine\s+company.*|ltd\.?|lda\.?)$",
    re.IGNORECASE,
)


@dataclass
class VivinoMatch:
    wine_id: int
    name: str
    ratings_average: float
    ratings_count: int
    wine_url: str


def _to_seo_name(producer: str) -> str:
    nfkd = unicodedata.normalize("NFKD", producer)
    ascii_str = nfkd.encode("ascii", "ignore").decode()
    cleaned = re.sub(r"[^a-z0-9\s-]", "", ascii_str.lower())
    return re.sub(r"\s+", "-", cleaned.strip())


def _seo_name_candidates(producer: str) -> list[str]:
    full = _to_seo_name(producer)
    # Strip prefix, suffix, then both
    no_prefix = _STRIP_PREFIXES.sub("", producer).strip()
    no_suffix = _STRIP_SUFFIXES.sub("", producer).strip()
    no_both = _STRIP_SUFFIXES.sub("", _STRIP_PREFIXES.sub("", producer).strip()).strip()

    seen: set[str] = set()
    candidates: list[str] = []
    for variant in (producer, no_prefix, no_suffix, no_both):
        slug = _to_seo_name(variant)
        if slug and slug not in seen:
            seen.add(slug)
            candidates.append(slug)
    return candidates


def _fetch_wines(seo_name: str, client: httpx.Client, cache: dict) -> list[dict] | None:
    """Return wine list for seo_name (cached), or None if not found."""
    if seo_name in cache:
        return cache[seo_name]
    url = _WINERY_URL.format(seo_name=seo_name)
    for attempt in range(_MAX_RETRIES):
        time.sleep(_REQUEST_DELAY)
        try:
            r = client.get(url, headers=_next_headers(), timeout=10)
        except httpx.RequestError as e:
            log.warning("Vivino request failed for %s: %s", seo_name, e)
            cache[seo_name] = None
            return None
        if r.status_code == 404:
            cache[seo_name] = None
            return None
        if r.status_code == 403:
            log.warning("Vivino 403 for %s (attempt %d/%d), backing off…", seo_name, attempt + 1, _MAX_RETRIES)
            time.sleep(_RETRY_DELAY)
            continue
        if r.status_code != 200:
            log.warning("Vivino returned %d for %s", r.status_code, seo_name)
            cache[seo_name] = None
            return None
        wines = r.json().get("wines", [])
        cache[seo_name] = wines
        return wines
    log.warning("Vivino 403 persisted for %s after %d retries, skipping.", seo_name, _MAX_RETRIES)
    cache[seo_name] = None
    return None


def lookup(
    producer: str,
    wine_name: str,
    client: httpx.Client,
    cache: dict | None = None,
) -> VivinoMatch | None:
    if cache is None:
        cache = {}
    wines: list[dict] | None = None
    for seo_name in _seo_name_candidates(producer):
        wines = _fetch_wines(seo_name, client, cache)
        if wines is not None:
            break

    if not wines:
        log.debug("No Vivino winery found for '%s'", producer)
        return None

    names = [w["name"] for w in wines]
    result = process.extractOne(wine_name, names, scorer=fuzz.WRatio, score_cutoff=75)
    if result is None:
        log.debug("No fuzzy match for '%s' among %s's wines", wine_name, producer)
        return None

    matched_name, score, idx = result
    log.debug("Vivino match '%s' → '%s' (score=%d)", wine_name, matched_name, score)
    w = wines[idx]
    stats = w.get("statistics", {})
    wine_id = w["id"]
    return VivinoMatch(
        wine_id=wine_id,
        name=matched_name,
        ratings_average=stats.get("ratings_average", 0.0),
        ratings_count=stats.get("ratings_count", 0),
        wine_url=_WINE_PAGE_URL.format(wine_id=wine_id),
    )
