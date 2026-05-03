import logging
import re
from datetime import date, datetime, timedelta, timezone

import httpx
import psycopg

from klunkar import config, db, ranking, systembolaget
from klunkar.models import RankedWine, Source, Subscriber, Wine
from klunkar.sources import ENRICHERS
from klunkar.telegram import send_message

log = logging.getLogger(__name__)


# ---- APIM key resolve ---------------------------------------------------


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


# ---- Wine ingest --------------------------------------------------------


def _wines_from_products(release_date: date, products: list[systembolaget.SBProduct]) -> list[Wine]:
    return [
        Wine(
            sb_product_number=p.product_number,
            sb_product_id=p.product_id,
            release_date=release_date,
            name=p.name,
            producer=p.producer,
            sb_url=p.product_url,
            price=p.price or None,
            wine_type=p.wine_type or None,
        )
        for p in products
    ]


# ---- Enrichment policy --------------------------------------------------


def _should_run(conn: psycopg.Connection, release_date: date, source: str) -> bool:
    last = db.get_last_run(conn, release_date, source)
    if last is None:
        return True
    run_at, matched = last
    age = datetime.now(timezone.utc) - run_at
    if matched == 0:
        # Retry-on-empty (handles late-publishing sources like Munskänkarna),
        # but respect the refresh interval so we don't hammer per cron tick.
        return age >= timedelta(hours=config.ENRICHMENT_REFRESH_HOURS)
    if release_date < date.today():
        return False
    return age >= timedelta(hours=config.ENRICHMENT_REFRESH_HOURS)


def _run_enrichers(
    conn: psycopg.Connection,
    release_date: date,
    client: httpx.Client,
    *,
    only: str | None = None,
    force: bool = False,
) -> dict[str, tuple[int, int]]:
    summary: dict[str, tuple[int, int]] = {}
    wines = db.get_wines(conn, release_date)
    if not wines:
        return summary
    for source_name, enricher in ENRICHERS.items():
        if only and source_name != only:
            continue
        if not force and not _should_run(conn, release_date, source_name):
            log.info("Skipping %s for %s (recent run)", source_name, release_date)
            continue
        try:
            results = enricher.enrich_release(release_date, wines, client, conn)
        except Exception:
            log.exception("Enricher %s failed for %s", source_name, release_date)
            continue
        db.upsert_enrichments(conn, release_date, source_name, results)
        db.record_enrichment_run(conn, release_date, source_name, len(results), len(wines))
        summary[source_name] = (len(results), len(wines))
        log.info(
            "Enricher %s for %s: matched %d/%d", source_name, release_date, len(results), len(wines)
        )
    return summary


# ---- Public orchestration -----------------------------------------------


def prefetch_upcoming(conn: psycopg.Connection, client: httpx.Client) -> None:
    """Scrape upcoming releases, persist wines, run all enrichers (idempotent)."""
    try:
        all_dates = systembolaget.scrape_release_dates(client)
    except Exception as e:
        log.error("Could not scrape release dates: %s", e)
        return

    today = date.today()
    horizon = today + timedelta(days=10)
    upcoming_dates = [d for d in all_dates if today <= d < horizon]
    db.save_release_dates(conn, upcoming_dates)

    for release_date in upcoming_dates:
        try:
            if not db.has_wines_for(conn, release_date):
                products = _fetch_with_key_refresh(release_date, conn, client)
                if not products:
                    continue
                db.upsert_wines(conn, _wines_from_products(release_date, products))
            _run_enrichers(conn, release_date, client)
        except Exception:
            log.exception("Prefetch failed for %s", release_date)

    # Backfill: revisit recent past releases. _should_run skips sources that
    # already matched, so this is cheap when nothing changed (typically a single
    # Munskänkarna fetch per release that was 0-match pre-release).
    backfill_since = today - timedelta(days=config.BACKFILL_WINDOW_DAYS)
    for past_date in db.get_past_release_dates_with_data(conn, since=backfill_since):
        try:
            _run_enrichers(conn, past_date, client)
        except Exception:
            log.exception("Backfill enrichment failed for %s", past_date)


def enrich_release(
    conn: psycopg.Connection,
    client: httpx.Client,
    release_date: date,
    *,
    only: str | None = None,
    force: bool = False,
) -> dict[str, tuple[int, int]]:
    """Run enrichers for one release on demand. Caller must ensure wines exist."""
    return _run_enrichers(conn, release_date, client, only=only, force=force)


def _notify_subscribers(
    conn: psycopg.Connection,
    release_date: date,
    subscribers: list[Subscriber],
    *,
    log_prefix: str = "",
    is_backfill: bool = False,
) -> int:
    """Send the ranked-view message to each eligible subscriber. Returns send count."""
    sent = 0
    for sub in subscribers:
        if db.has_notified_subscriber(conn, release_date, sub.chat_id):
            continue
        value_set = set(sub.value_filter) if sub.value_filter else None
        type_set = set(sub.wine_type_filter) if sub.wine_type_filter else None
        ranked = ranking.build_ranked_view(
            conn,
            release_date,
            source=sub.rank_source,
            value_ratings=value_set,
            wine_types=type_set,
        )
        if not ranked:
            log.info(
                "%sNo %s-ranked wines for %s — skipping chat %d",
                log_prefix,
                sub.rank_source,
                release_date,
                sub.chat_id,
            )
            continue
        try:
            send_message(
                sub.chat_id,
                format_message(
                    ranked,
                    release_date,
                    source=sub.rank_source,
                    max_price=sub.max_price,
                    value_ratings=value_set,
                    wine_types=type_set,
                    is_backfill=is_backfill,
                ),
            )
            db.mark_notified_subscriber(conn, release_date, sub.chat_id)
            sent += 1
        except Exception as e:
            log.error("%sFailed to send to %d: %s", log_prefix, sub.chat_id, e)
    return sent


def check_and_notify(conn: psycopg.Connection) -> bool:
    """Notify subscribers about tomorrow's release, then any retroactive sends.

    Retroactive sends cover subscribers who joined before a recent past release
    but were skipped at the time because their chosen source had no data
    (typically Munskänkarna pre-publication).
    """
    notified_total = 0
    today = date.today()

    # Tomorrow's release
    tomorrow = today + timedelta(1)
    if db.is_upcoming_release_date(conn, tomorrow):
        sent = _notify_subscribers(conn, tomorrow, db.get_subscribers(conn))
        notified_total += sent
        if sent and not db.is_release_seen(conn, tomorrow):
            wines_total = len(db.get_wines(conn, tomorrow))
            db.mark_release_seen(conn, tomorrow, wines_total)

    # Retroactive: past releases inside the backfill window
    backfill_since = today - timedelta(days=config.BACKFILL_WINDOW_DAYS)
    for past_date in db.get_past_release_dates_with_data(conn, since=backfill_since):
        eligible = db.get_subscribers_to_notify_for(conn, past_date)
        if not eligible:
            continue
        sent = _notify_subscribers(
            conn, past_date, eligible, log_prefix="[backfill] ", is_backfill=True
        )
        if sent:
            log.info("Retro-notified %d subscribers for %s", sent, past_date)
            notified_total += sent
            if not db.is_release_seen(conn, past_date):
                wines_total = len(db.get_wines(conn, past_date))
                db.mark_release_seen(conn, past_date, wines_total)

    return notified_total > 0


# ---- Message formatting -------------------------------------------------

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

_WINE_GLASS = {
    "Rött vin": "🍷",
    "Vitt vin": "🥂",
    "Rosévin": "🌸",
    "Mousserande vin": "🍾",
    "Starkvin": "🥃",
    "Smaksatt vin & fruktvin": "🍇",
    "Aperitifer": "🍸",
    "Vermouth": "🍸",
}
_DEFAULT_GLASS = "🍷"


def _escape(text: str) -> str:
    return _MDV2_SPECIAL.sub(r"\\\1", text)


def _sv_date(d: date) -> str:
    return f"{d.day} {_MONTHS_SV[d.month - 1]} {d.year}"


def _source_label(source: Source | str) -> str:
    enricher = ENRICHERS.get(Source(source))
    return enricher.display_name if enricher else str(source)


def format_message(
    wines: list[RankedWine],
    release_date: date,
    *,
    source: Source | str,
    max_price: float | None = None,
    value_ratings: set[str] | None = None,
    wine_types: set[str] | None = None,
    is_backfill: bool = False,
) -> str:
    source = Source(source)
    if max_price:
        wines = [w for w in wines if (w.wine.price or 0) <= max_price]
    wines = wines[: config.TOP_N]

    date_str = _escape(_sv_date(release_date))
    lines: list[str] = []
    if is_backfill:
        lines.append(f"📬 *Uppdaterad lista* — {_escape(_source_label(source))} finns nu med.")
        lines.append("")
    lines.append(f"🍷 *Tillfälligt sortiment — {date_str}*")
    lines.append(_escape(f"Rankas av {_source_label(source)}"))
    if max_price:
        lines.append(_escape(f"Budget: {int(max_price)} kr"))
    if wine_types:
        types = ", ".join(sorted(wine_types))
        lines.append(_escape(f"Vintyp: {types}"))
    if value_ratings:
        cats = ", ".join(sorted(value_ratings))
        lines.append(_escape(f"Kategori: {cats}"))
    lines.append("")

    for w in wines:
        wine = w.wine
        name = _escape(wine.name)
        glass = _WINE_GLASS.get(wine.wine_type or "", _DEFAULT_GLASS)
        lines.append(f"{glass} {name}")

        price_text = f"{int(wine.price)} kr" if wine.price else "köp"
        sb_label = _escape(f"Systembolaget: {price_text}")
        lines.append(f"[{sb_label}]({wine.sb_url})")

        if w.vivino:
            label = _escape(f"Vivino: {w.vivino.ratings_average:.1f} ★")
            lines.append(f"[{label}]({w.vivino.wine_url})")

        if w.munskankarna:
            chunk = f"Munskänkarna: {w.munskankarna.score:g}/20"
            if w.munskankarna.value_rating:
                chunk += f" ({w.munskankarna.value_rating})"
            label = _escape(chunk)
            url = w.munskankarna.review_url
            lines.append(f"[{label}]({url})" if url else label)

        lines.append("")

    return "\n".join(lines)
