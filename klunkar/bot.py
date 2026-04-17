import logging
import time

import httpx

from klunkar import config, db
from klunkar.release import RankedWine, format_message
from klunkar.telegram import send_message

log = logging.getLogger(__name__)

_POLL_TIMEOUT = 30
_WELCOME = (
    "🍷 *Välkommen till Klunkar\\!*\n\n"
    "Dagen innan varje tillfälligt sortiment på Systembolaget får du de tio "
    "bäst betygsatta vinerna enligt Vivino\\.\n\n"
    "Använd /budget för att filtrera på maxpris, t\\.ex\\. /budget 150\\."
)


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


def _handle_update(update: dict, conn) -> None:
    msg = update.get("message", {})
    text = msg.get("text", "")
    chat_id = msg.get("chat", {}).get("id")
    if not chat_id or not text:
        return

    if text.startswith("/start"):
        new = db.add_subscriber(conn, chat_id)
        send_message(chat_id, _WELCOME)
        if new:
            result = db.get_last_release_wines(conn, max_age_days=None)
            if result:
                release_date, rows = result
                wines = [
                    RankedWine(
                        rank=r[0],
                        name=r[1],
                        score=r[2],
                        vivino_url=r[3],
                        sb_url=r[4],
                        price=r[5] or 0.0,
                        wine_type=r[6] or "",
                    )
                    for r in rows
                ]
                max_price = db.get_subscriber_budget(conn, chat_id)
                send_message(chat_id, format_message(wines, release_date, max_price=max_price))
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
        result = db.get_last_release_wines(conn, max_age_days=None)
        if result:
            release_date, rows = result
            wines = [
                RankedWine(
                    rank=r[0], name=r[1], score=r[2], vivino_url=r[3],
                    sb_url=r[4], price=r[5] or 0.0, wine_type=r[6] or "",
                )
                for r in rows
            ]
            send_message(chat_id, format_message(wines, release_date, max_price=max_price))
        log.info("/budget from %d", chat_id)

    elif text.startswith("/help"):
        send_message(
            chat_id,
            "🍷 *Klunkar* hjälper dig hitta de bästa vinerna från Systembolagets tillfälliga sortiment\\.\n\n"
            "*/start* — prenumerera\n"
            "*/stop* — avsluta\n"
            "*/budget 150* — visa viner under 150 kr\n"
            "*/budget* — ta bort budgetfilter",
        )
        log.info("/help from %d", chat_id)

    elif text.startswith("/stop"):
        removed = db.remove_subscriber(conn, chat_id)
        reply = "Du är nu avprenumererad\\. Skriv /start för att prenumerera igen\\."
        send_message(chat_id, reply)
        log.info("/stop from %d (removed=%s)", chat_id, removed)


def run() -> None:
    import os
    log.info("ENV DUMP: %s", {k: v for k, v in os.environ.items() if "TOKEN" not in k and "PASSWORD" not in k and "URL" not in k})
    log.info("TOKEN set: %s, DB set: %s", bool(config.TELEGRAM_BOT_TOKEN), bool(config.DATABASE_URL))
    if not config.TELEGRAM_BOT_TOKEN:
        raise RuntimeError("TELEGRAM_BOT_TOKEN is not set")

    base = f"https://api.telegram.org/bot{config.TELEGRAM_BOT_TOKEN}"
    offset = 0

    with db.get_conn() as conn:
        db.migrate(conn)
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
                    offset = update["update_id"] + 1
