import logging
import time
from datetime import date, timedelta

import httpx
import psycopg

from klunkar import config, db, ranking
from klunkar.release import _escape, _source_label, _sv_date, format_message
from klunkar.sources import ENRICHERS
from klunkar.telegram import send_message

log = logging.getLogger(__name__)

_POLL_TIMEOUT = 30
_WELCOME = (
    "🍷 *Välkommen till Klunkar\\!*\n\n"
    "Dagen innan varje släpp av tillfälligt sortiment på Systembolaget får du de tio "
    "bäst betygsatta vinerna\\.\n\n"
    "Använd /budget för att filtrera på maxpris, t\\.ex\\. /budget 150\\.\n"
    "Använd /source för att välja om listan rankas av Vivino eller Munskänkarna\\.\n"
    "Använd /category för att filtrera på Munskänkarnas kategori, t\\.ex\\. /category fynd\\."
)

_VALUE_CANONICAL = ["fynd", "mer än prisvärt", "prisvärt", "ej prisvärt"]
_VALUE_ALIASES = {
    "fynd": "fynd",
    "mer": "mer än prisvärt",
    "mer än prisvärt": "mer än prisvärt",
    "mer-an-prisvart": "mer än prisvärt",
    "prisv": "prisvärt",
    "prisvärt": "prisvärt",
    "prisvart": "prisvärt",
    "ej": "ej prisvärt",
    "ej prisvärt": "ej prisvärt",
    "ej-prisvart": "ej prisvärt",
}
_CATEGORY_CLEAR_TOKENS = {"clear", "off", "none", "-", "rensa", "ta-bort"}


def parse_category_args(arg: str) -> tuple[list[str], list[str]]:
    """Returns (resolved, unknown). Empty resolved + empty unknown = clear."""
    raw = arg.strip().lower()
    if not raw or raw in _CATEGORY_CLEAR_TOKENS:
        return [], []
    tokens = [t.strip() for t in raw.split(",") if t.strip()]
    resolved: list[str] = []
    unknown: list[str] = []
    for tok in tokens:
        canon = _VALUE_ALIASES.get(tok)
        if canon is None:
            unknown.append(tok)
        elif canon not in resolved:
            resolved.append(canon)
    return resolved, unknown


def _get_updates(base: str, client: httpx.Client, offset: int) -> list[dict]:
    r = client.get(
        f"{base}/getUpdates",
        params={
            "timeout": _POLL_TIMEOUT,
            "offset": offset,
            "allowed_updates": ["message"],
        },
        timeout=_POLL_TIMEOUT + 10,
    )
    r.raise_for_status()
    return r.json().get("result", [])


def _send_ranked(
    chat_id: int, conn: psycopg.Connection, release_date: date, source: str
) -> bool:
    value_filter = db.get_subscriber_value_filter(conn, chat_id)
    value_set = set(value_filter) if value_filter else None
    ranked = ranking.build_ranked_view(
        conn, release_date, source=source, value_ratings=value_set,
    )
    if not ranked:
        return False
    max_price = db.get_subscriber_budget(conn, chat_id)
    send_message(
        chat_id,
        format_message(
            ranked, release_date,
            source=source, max_price=max_price, value_ratings=value_set,
        ),
    )
    return True


def _resolve_active_date(conn: psycopg.Connection) -> date | None:
    tomorrow = date.today() + timedelta(days=1)
    if db.has_wines_for(conn, tomorrow):
        return tomorrow
    return db.get_last_release_with_data(conn)


def _handle_start(chat_id: int, conn: psycopg.Connection) -> None:
    new = db.add_subscriber(conn, chat_id)
    send_message(chat_id, _WELCOME)
    if new:
        target = _resolve_active_date(conn)
        if target:
            source = db.get_subscriber_rank_source(conn, chat_id)
            _send_ranked(chat_id, conn, target, source)
    log.info("/start from %d (new=%s)", chat_id, new)


def _handle_budget(chat_id: int, text: str, conn: psycopg.Connection) -> None:
    parts = text.split()
    if len(parts) >= 2:
        try:
            max_price = float(parts[1])
            db.set_subscriber_budget(conn, chat_id, max_price)
            send_message(chat_id, f"Budget satt till {int(max_price)} kr\\.")
        except ValueError:
            send_message(chat_id, "Ange ett giltigt belopp, t\\.ex\\. /budget 150\\.")
            return
    else:
        db.set_subscriber_budget(conn, chat_id, None)
        send_message(chat_id, "Budget borttagen \\— du får nu alla tio bästa vinerna\\.")

    target = db.get_subscriber_preview_date(conn, chat_id) or _resolve_active_date(conn)
    if target:
        source = db.get_subscriber_rank_source(conn, chat_id)
        _send_ranked(chat_id, conn, target, source)
    log.info("/budget from %d", chat_id)


def _handle_source(chat_id: int, text: str, conn: psycopg.Connection) -> None:
    parts = text.split()
    target = _resolve_active_date(conn)
    available = db.get_available_sources_for(conn, target) if target else []
    valid = list(ENRICHERS.keys())

    if len(parts) < 2:
        current = db.get_subscriber_rank_source(conn, chat_id)
        lines = [
            f"*Aktuell källa:* {_escape(_source_label(current))}",
            "",
            "*Tillgängliga källor för nästa släpp:*",
        ]
        if not available:
            lines.append(_escape("Inga källor är tillgängliga ännu."))
        else:
            for s in available:
                lines.append(f"• {_escape(_source_label(s))} — `/source {s}`")
        send_message(chat_id, "\n".join(lines))
        return

    choice = parts[1].strip().lower()
    if choice not in valid:
        send_message(
            chat_id,
            _escape(
                f"Okänd källa '{choice}'. Giltiga: {', '.join(valid)}."
            ),
        )
        return

    db.set_subscriber_rank_source(conn, chat_id, choice)
    send_message(chat_id, f"Källa satt till *{_escape(_source_label(choice))}*\\.")

    if target and not _send_ranked(chat_id, conn, target, choice):
        send_message(
            chat_id,
            _escape(
                f"{_source_label(choice)} har inga viner för nästa släpp ännu — "
                "du får din lista när den landar."
            ),
        )
    log.info("/source %s from %d", choice, chat_id)


def _handle_category(chat_id: int, text: str, conn: psycopg.Connection) -> None:
    parts = text.split(maxsplit=1)
    arg = parts[1] if len(parts) >= 2 else ""

    if not arg.strip():
        current = db.get_subscriber_value_filter(conn, chat_id) or []
        lines = ["*Munskänkarnas kategorier*", ""]
        if current:
            lines.append(f"Aktiv: {_escape(', '.join(current))}")
        else:
            lines.append(_escape("Aktiv: ingen (alla kategorier)"))
        lines.append("")
        lines.append("*Tillgängliga:*")
        for v in _VALUE_CANONICAL:
            lines.append(f"• {_escape(v)}")
        lines.append("")
        lines.append(_escape("Sätt med t.ex. /category fynd  eller  /category fynd,prisvärt"))
        lines.append(_escape("Rensa med /category clear"))
        send_message(chat_id, "\n".join(lines))
        return

    resolved, unknown = parse_category_args(arg)
    if unknown:
        send_message(
            chat_id,
            _escape(
                f"Okänd kategori: {', '.join(unknown)}. "
                f"Giltiga: {', '.join(_VALUE_CANONICAL)}."
            ),
        )
        return

    if not resolved:
        db.set_subscriber_value_filter(conn, chat_id, None)
        send_message(chat_id, _escape("Kategorifilter borttaget — du får alla kategorier."))
    else:
        db.set_subscriber_value_filter(conn, chat_id, resolved)
        send_message(
            chat_id,
            _escape(f"Kategorifilter satt till: {', '.join(resolved)}."),
        )

    target = db.get_subscriber_preview_date(conn, chat_id) or _resolve_active_date(conn)
    if target:
        source = db.get_subscriber_rank_source(conn, chat_id)
        if not _send_ranked(chat_id, conn, target, source):
            send_message(
                chat_id,
                _escape("Inga viner matchar de filter du valt för nästa släpp."),
            )
    log.info("/category from %d → %s", chat_id, resolved or "[cleared]")


def _handle_preview(chat_id: int, text: str, conn: psycopg.Connection) -> None:
    parts = text.split()
    if len(parts) >= 2:
        try:
            target = date.fromisoformat(parts[1])
        except ValueError:
            send_message(chat_id, "Ange ett datum, t\\.ex\\. /preview 2026\\-05\\-08\\.")
            return
    else:
        upcoming = db.get_upcoming_release_dates(conn, date.today())
        if not upcoming:
            send_message(chat_id, "Inga kommande släpp hittades inom de närmaste 90 dagarna\\.")
            return
        target = upcoming[0]

    if not db.has_wines_for(conn, target):
        send_message(
            chat_id,
            f"Inga viner finns ännu för {_escape(_sv_date(target))}\\. Försök igen senare\\.",
        )
        return

    db.set_subscriber_preview_date(conn, chat_id, target)
    source = db.get_subscriber_rank_source(conn, chat_id)
    if not _send_ranked(chat_id, conn, target, source):
        send_message(
            chat_id,
            _escape(
                f"{_source_label(source)} har inga viner för {_sv_date(target)} ännu."
            ),
        )
    log.info("/preview %s from %d", target, chat_id)


def _handle_releases(chat_id: int, conn: psycopg.Connection) -> None:
    dates = db.get_upcoming_release_dates(conn, date.today())
    if not dates:
        send_message(chat_id, "Inga kommande släpp hittades\\.")
    else:
        lines = ["*Kommande släpp*", ""]
        for d in dates:
            lines.append(f"• {_escape(_sv_date(d))}")
        send_message(chat_id, "\n".join(lines))
    log.info("/releases from %d", chat_id)


def _handle_help(chat_id: int) -> None:
    send_message(
        chat_id,
        "🍷 *Klunkar* hjälper dig hitta de bästa vinerna från Systembolagets tillfälliga sortiment\\.\n\n"
        "*/start* — prenumerera\n"
        "*/stop* — avsluta\n"
        "*/releases* — kommande släpp\n"
        "*/preview* — visa viner för nästa släpp\n"
        "*/preview 2026\\-05\\-08* — visa viner för ett specifikt datum\n"
        "*/budget 150* — visa viner under 150 kr\n"
        "*/budget* — ta bort budgetfilter\n"
        "*/source* — visa eller välj rankningskälla \\(vivino, munskankarna\\)\n"
        "*/category fynd* — filtrera på Munskänkarnas kategori\n"
        "*/category clear* — ta bort kategorifilter",
    )
    log.info("/help from %d", chat_id)


def _handle_stop(chat_id: int, conn: psycopg.Connection) -> None:
    removed = db.remove_subscriber(conn, chat_id)
    send_message(chat_id, "Du är nu avprenumererad\\. Skriv /start för att prenumerera igen\\.")
    log.info("/stop from %d (removed=%s)", chat_id, removed)


def _handle_update(update: dict, conn: psycopg.Connection) -> None:
    msg = update.get("message", {})
    text = msg.get("text", "")
    chat_id = msg.get("chat", {}).get("id")
    if not chat_id or not text:
        return

    if text.startswith("/start"):
        _handle_start(chat_id, conn)
    elif text.startswith("/budget"):
        _handle_budget(chat_id, text, conn)
    elif text.startswith("/source"):
        _handle_source(chat_id, text, conn)
    elif text.startswith("/category"):
        _handle_category(chat_id, text, conn)
    elif text.startswith("/preview"):
        _handle_preview(chat_id, text, conn)
    elif text.startswith("/releases"):
        _handle_releases(chat_id, conn)
    elif text.startswith("/help"):
        _handle_help(chat_id)
    elif text.startswith("/stop"):
        _handle_stop(chat_id, conn)


def run() -> None:
    if not config.TELEGRAM_BOT_TOKEN:
        raise RuntimeError("TELEGRAM_BOT_TOKEN is not set")

    base = f"https://api.telegram.org/bot{config.TELEGRAM_BOT_TOKEN}"
    offset = 0

    with db.get_conn() as conn:
        log.info("Bot started, long-polling…")
        with httpx.Client() as client:
            while True:
                try:
                    updates = _get_updates(base, client, offset)
                except Exception as e:
                    log.warning("Poll error: %s — retrying in 5s", e)
                    time.sleep(5)
                    continue

                for update in updates:
                    try:
                        _handle_update(update, conn)
                    except Exception as e:
                        log.error("Error handling update %s: %s", update.get("update_id"), e)
                        conn.rollback()
                    offset = update["update_id"] + 1
