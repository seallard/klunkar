import itertools
import logging
import re
import time
import unicodedata
from dataclasses import dataclass

import httpx
from rapidfuzz import fuzz, process

log = logging.getLogger(__name__)

_WINERY_URL = "https://www.vivino.com/api/wineries/{slug}/wines"
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
_REQUEST_DELAY = 0.3
_RETRY_DELAY = 10.0
_MAX_RETRIES = 2

_ua_cycle = itertools.cycle(_USER_AGENTS)

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


def _slugify(name: str) -> str:
    nfkd = unicodedata.normalize("NFKD", name)
    ascii_str = nfkd.encode("ascii", "ignore").decode()
    cleaned = re.sub(r"[^a-z0-9\s-]", "", ascii_str.lower())
    return re.sub(r"\s+", "-", cleaned.strip())


def _slug_candidates(producer: str) -> list[str]:
    no_prefix = _STRIP_PREFIXES.sub("", producer).strip()
    no_suffix = _STRIP_SUFFIXES.sub("", producer).strip()
    no_both = _STRIP_SUFFIXES.sub("", no_prefix).strip()
    slugs = (_slugify(v) for v in (producer, no_prefix, no_suffix, no_both))
    return list(dict.fromkeys(s for s in slugs if s))


def _next_headers() -> dict:
    return {
        "User-Agent": next(_ua_cycle),
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://www.vivino.com/",
    }


def _fetch_wines(
    slug: str, client: httpx.Client, cache: dict[str, list[dict] | None]
) -> list[dict] | None:
    if slug in cache:
        return cache[slug]
    url = _WINERY_URL.format(slug=slug)
    for attempt in range(_MAX_RETRIES):
        time.sleep(_REQUEST_DELAY)
        try:
            r = client.get(url, headers=_next_headers(), timeout=10)
        except httpx.RequestError as e:
            log.warning("Vivino request failed for %s: %s", slug, e)
            cache[slug] = None
            return None
        if r.status_code == 404:
            cache[slug] = None
            return None
        if r.status_code == 403:
            log.warning("Vivino 403 for %s (attempt %d/%d), backing off…", slug, attempt + 1, _MAX_RETRIES)
            time.sleep(_RETRY_DELAY)
            continue
        if r.status_code != 200:
            log.warning("Vivino returned %d for %s", r.status_code, slug)
            cache[slug] = None
            return None
        wines = r.json().get("wines", [])
        cache[slug] = wines
        return wines
    log.warning("Vivino 403 persisted for %s after %d retries, skipping.", slug, _MAX_RETRIES)
    cache[slug] = None
    return None


def lookup(
    producer: str,
    wine_name: str,
    client: httpx.Client,
    cache: dict[str, list[dict] | None],
) -> VivinoMatch | None:
    wines = next(
        (w for slug in _slug_candidates(producer) if (w := _fetch_wines(slug, client, cache)) is not None),
        None,
    )
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
    return VivinoMatch(
        wine_id=w["id"],
        name=matched_name,
        ratings_average=stats.get("ratings_average", 0.0),
        ratings_count=stats.get("ratings_count", 0),
        wine_url=_WINE_PAGE_URL.format(wine_id=w["id"]),
    )
