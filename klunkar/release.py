import logging
import re
from dataclasses import dataclass
from datetime import date

import httpx
import psycopg

from klunkar import config, db, systembolaget, vivino

log = logging.getLogger(__name__)


@dataclass
class RankedWine:
    rank: int
    name: str
    score: float
    vivino_url: str
    sb_url: str
    price: float
    wine_type: str


def _bayesian_score(r: float, v: int, c: float, m: int) -> float:
    return (v / (v + m)) * r + (m / (v + m)) * c


def _resolve_apim_key(conn: psycopg.Connection, client: httpx.Client) -> str:
    key = db.get_apim_key(conn)
    if not key:
        key = systembolaget.scrape_apim_key(client)
        db.set_apim_key(conn, key)
    return key


def _fetch_with_key_refresh(
    release_date: date,
    conn: psycopg.Connection,
    client: httpx.Client,
) -> list[systembolaget.SBProduct]:
    key = _resolve_apim_key(conn, client)
    try:
        return systembolaget.fetch_release_products(release_date, key, client)
    except PermissionError:
        log.info("APIM key rejected, re-scraping…")
        key = systembolaget.scrape_apim_key(client)
        db.set_apim_key(conn, key)
        return systembolaget.fetch_release_products(release_date, key, client)


def rank_release(products: list[systembolaget.SBProduct], client: httpx.Client) -> list[RankedWine]:
    matches: list[tuple[systembolaget.SBProduct, vivino.VivinoMatch]] = []
    vivino_cache: dict = {}

    for product in products:
        match = vivino.lookup(product.producer, product.name, client, vivino_cache)
        if match is None:
            log.info("No Vivino match for '%s' (%s)", product.name, product.producer)
            continue
        matches.append((product, match))

    if not matches:
        return []

    c = sum(m.ratings_average for _, m in matches) / len(matches)
    m_prior = config.VIVINO_RATING_PRIOR

    scored = [
        (
            product,
            match,
            _bayesian_score(match.ratings_average, match.ratings_count, c, m_prior),
        )
        for product, match in matches
    ]
    scored.sort(key=lambda t: t[2], reverse=True)

    return [
        RankedWine(
            rank=i + 1,
            name=product.name,
            score=score,
            vivino_url=match.wine_url,
            sb_url=product.product_url,
            price=product.price,
            wine_type=product.wine_type,
        )
        for i, (product, match, score) in enumerate(scored)
    ]


_MDV2_SPECIAL = re.compile(r"([_*\[\]()~`>#+\-=|{}.!\\])")

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

_MEDALS = {1: "🥇", 2: "🥈", 3: "🥉"}
_WINE_GLASS = {"Rött vin": "🍷", "Vitt vin": "🥂", "Rosévin": "🌸"}
_DEFAULT_GLASS = "🍷"


def _escape(text: str) -> str:
    return _MDV2_SPECIAL.sub(r"\\\1", text)


def _sv_date(d: date) -> str:
    return f"{d.day} {_MONTHS_SV[d.month - 1]} {d.year}"


def format_message(
    wines: list[RankedWine], release_date: date, max_price: float | None = None
) -> str:
    if max_price is not None:
        wines = [w for w in wines if w.price <= max_price]
    wines = wines[: config.TOP_N]
    date_str = _escape(_sv_date(release_date))
    lines = [f"🍷 *Tillfälligt sortiment \u2014 {date_str}*"]
    if max_price is not None:
        lines[0] += f" \\(max {_escape(f'{int(max_price)} kr')}\\)"
    lines.append("")
    for w in wines:
        name = _escape(w.name)
        medal = _MEDALS.get(w.rank, "")
        prefix = medal if medal else _WINE_GLASS.get(w.wine_type, _DEFAULT_GLASS)
        score = _escape(f"{w.score:.1f}")
        link_text = f"{name} \\({score}\\)"
        lines.append(f"{prefix} [{link_text}]({w.vivino_url})")
        price_text = _escape(f"{int(w.price)} kr") if w.price else "köp"
        lines.append(f"🛒 [{price_text}]({w.sb_url})")
        lines.append("")
    return "\n".join(lines)


def prefetch_upcoming(conn: psycopg.Connection, client: httpx.Client) -> None:
    """Fetch and cache scored wines for all upcoming releases (next 90 days)."""
    from datetime import timedelta

    today = date.today()
    try:
        all_dates = systembolaget.scrape_release_dates(client)
    except Exception as e:
        log.error("Could not scrape release dates: %s", e)
        return
    upcoming = [d for d in all_dates if d <= today + timedelta(days=10)]

    db.save_release_dates(conn, upcoming)

    for release_date in upcoming:
        if db.get_release_wines(conn, release_date) is not None:
            log.info("Release %s already cached, skipping prefetch.", release_date)
            continue
        try:
            products = _fetch_with_key_refresh(release_date, conn, client)
            if not products:
                log.info("No products for %s, skipping.", release_date)
                continue
            wines = rank_release(products, client)
            if wines:
                db.save_release_wines(conn, release_date, wines)
                log.info("Prefetched %d wines for %s.", len(wines), release_date)
        except Exception as e:
            log.error("Prefetch failed for %s: %s", release_date, e)


def check_and_notify(conn: psycopg.Connection, client: httpx.Client, release_date: date) -> bool:
    """Return True if a release was found and notifications sent."""
    if not db.is_upcoming_release_date(conn, release_date):
        log.info("No release on %s, exiting quietly.", release_date)
        return False

    if db.is_release_seen(conn, release_date):
        log.info("Release %s already notified, skipping.", release_date)
        return False

    products = _fetch_with_key_refresh(release_date, conn, client)
    wines = rank_release(products, client)
    db.save_release_wines(conn, release_date, wines)

    if not wines:
        log.warning(
            "Release on %s but no wines could be scored — skipping notification.",
            release_date,
        )
        return False

    subscribers = db.get_subscribers(conn)
    log.info("Sending to %d subscribers.", len(subscribers))

    from klunkar.telegram import send_message  # avoid circular import at module level

    failed = 0
    for chat_id, max_price in subscribers:
        try:
            send_message(chat_id, format_message(wines, release_date, max_price=max_price))
        except Exception as e:
            log.error("Failed to send to %d: %s", chat_id, e)
            failed += 1

    db.mark_release_seen(conn, release_date, len(wines))
    log.info("Done. Sent to %d/%d subscribers.", len(subscribers) - failed, len(subscribers))
    return True
