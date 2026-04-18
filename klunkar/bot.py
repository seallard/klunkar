import logging
import time
from datetime import date, timedelta

import httpx
import psycopg

from klunkar import config, db
from klunkar.release import RankedWine, _escape, _sv_date, format_message
from klunkar.telegram import send_message

log = logging.getLogger(__name__)

_POLL_TIMEOUT = 30
_WELCOME = (
    "🍷 *Välkommen till Klunkar\\!*\n\n"
    "Dagen innan varje släpp av tillfälligt sortiment på Systembolaget får du de tio "
    "bäst betygsatta vinerna enligt Vivino\\.\n\n"
    "Använd /budget för att filtrera på maxpris, t\\.ex\\. /budget 150\\."
)


def _wines_from_rows(rows) -> list[RankedWine]:
    return [
        RankedWine(
            rank=r[0], name=r[1], score=r[2], vivino_url=r[3],
            sb_url=r[4], price=r[5] or 0.0, wine_type=r[6] or "",
        )
        for r in rows
    ]


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


def _handle_update(update: dict, conn: psycopg.Connection) -> None:
    msg = update.get("message", {})
    text = msg.get("text", "")
    chat_id = msg.get("chat", {}).get("id")
    if not chat_id or not text:
        return

    if text.startswith("/start"):
        new = db.add_subscriber(conn, chat_id)
        send_message(chat_id, _WELCOME)
        if new:
            tomorrow = date.today() + timedelta(days=1)
            rows = db.get_release_wines(conn, tomorrow)
            release_date = tomorrow if rows else None
            if not rows:
                result = db.get_last_release_wines(conn, max_age_days=None)
                if result:
                    release_date, rows = result
            if rows:
                max_price = db.get_subscriber_budget(conn, chat_id)
                send_message(chat_id, format_message(_wines_from_rows(rows), release_date, max_price=max_price))
        log.info("/start from %d (new=%s)", chat_id, new)

    elif text.startswith("/budget"):
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
            max_price = None
            db.set_subscriber_budget(conn, chat_id, None)
            send_message(chat_id, "Budget borttagen \\— du får nu alla tio bästa vinerna\\.")
        preview_date = db.get_subscriber_preview_date(conn, chat_id)
        if preview_date:
            rows = db.get_release_wines(conn, preview_date)
            if rows:
                send_message(chat_id, format_message(_wines_from_rows(rows), preview_date, max_price=max_price))
                log.info("/budget from %d", chat_id)
                return
        result = db.get_last_release_wines(conn, max_age_days=None)
        if result:
            release_date, rows = result
            send_message(chat_id, format_message(_wines_from_rows(rows), release_date, max_price=max_price))
        log.info("/budget from %d", chat_id)

    elif text.startswith("/preview"):
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

        rows = db.get_release_wines(conn, target)
        if rows is None:
            send_message(
                chat_id,
                f"Inga viner finns ännu för {_escape(_sv_date(target))}\\. Försök igen senare\\.",
            )
            return
        max_price = db.get_subscriber_budget(conn, chat_id)
        db.set_subscriber_preview_date(conn, chat_id, target)
        send_message(chat_id, format_message(_wines_from_rows(rows), target, max_price=max_price))
        log.info("/preview %s from %d", target, chat_id)

    elif text.startswith("/releases"):
        dates = db.get_upcoming_release_dates(conn, date.today())
        if not dates:
            send_message(chat_id, "Inga kommande släpp hittades\\.")
        else:
            lines = ["*Kommande släpp:*", ""]
            for d in dates:
                lines.append(f"• {_escape(_sv_date(d))}")
            send_message(chat_id, "\n".join(lines))
        log.info("/releases from %d", chat_id)

    elif text.startswith("/help"):
        send_message(
            chat_id,
            "🍷 *Klunkar* hjälper dig hitta de bästa vinerna från Systembolagets tillfälliga sortiment\\.\n\n"
            "*/start* — prenumerera\n"
            "*/stop* — avsluta\n"
            "*/releases* — kommande släpp\n"
            "*/preview* — visa viner för nästa släpp\n"
            "*/preview 2026\\-05\\-08* — visa viner för ett specifikt datum\n"
            "*/budget 150* — visa viner under 150 kr\n"
            "*/budget* — ta bort budgetfilter",
        )
        log.info("/help from %d", chat_id)

    elif text.startswith("/stop"):
        removed = db.remove_subscriber(conn, chat_id)
        send_message(chat_id, "Du är nu avprenumererad\\. Skriv /start för att prenumerera igen\\.")
        log.info("/stop from %d (removed=%s)", chat_id, removed)


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
