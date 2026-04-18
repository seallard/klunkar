import logging

import httpx

from klunkar import config

log = logging.getLogger(__name__)


def send_message(chat_id: int, text: str) -> None:
    if not config.TELEGRAM_BOT_TOKEN:
        raise RuntimeError("TELEGRAM_BOT_TOKEN is not set")
    base = f"https://api.telegram.org/bot{config.TELEGRAM_BOT_TOKEN}"
    with httpx.Client() as client:
        r = client.post(
            f"{base}/sendMessage",
            json={
                "chat_id": chat_id,
                "text": text,
                "parse_mode": "MarkdownV2",
                "disable_web_page_preview": True,
            },
        )
        if not r.is_success:
            log.error("Telegram sendMessage failed %d: %s | text=%r", r.status_code, r.text, text)
        r.raise_for_status()
