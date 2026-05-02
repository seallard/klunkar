import logging

import httpx

from klunkar import config

log = logging.getLogger(__name__)


def _base() -> str:
    if not config.TELEGRAM_BOT_TOKEN:
        raise RuntimeError("TELEGRAM_BOT_TOKEN is not set")
    return f"https://api.telegram.org/bot{config.TELEGRAM_BOT_TOKEN}"


def send_message(chat_id: int, text: str, reply_markup: dict | None = None) -> None:
    payload: dict = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "MarkdownV2",
        "disable_web_page_preview": True,
    }
    if reply_markup is not None:
        payload["reply_markup"] = reply_markup
    with httpx.Client() as client:
        r = client.post(f"{_base()}/sendMessage", json=payload)
        if not r.is_success:
            log.error("Telegram sendMessage failed %d: %s | text=%r", r.status_code, r.text, text)
        r.raise_for_status()


def edit_message_text(
    chat_id: int, message_id: int, text: str, reply_markup: dict | None = None
) -> None:
    payload: dict = {
        "chat_id": chat_id,
        "message_id": message_id,
        "text": text,
        "parse_mode": "MarkdownV2",
        "disable_web_page_preview": True,
    }
    if reply_markup is not None:
        payload["reply_markup"] = reply_markup
    with httpx.Client() as client:
        r = client.post(f"{_base()}/editMessageText", json=payload)
        if not r.is_success:
            # Telegram returns 400 when the new content is identical to the
            # current message (e.g. user double-taps). Treat that as a no-op.
            if "message is not modified" in r.text:
                return
            log.error("Telegram editMessageText failed %d: %s", r.status_code, r.text)
        r.raise_for_status()


def answer_callback_query(callback_query_id: str, text: str | None = None) -> None:
    payload: dict = {"callback_query_id": callback_query_id}
    if text is not None:
        payload["text"] = text
    with httpx.Client() as client:
        r = client.post(f"{_base()}/answerCallbackQuery", json=payload)
        if not r.is_success:
            log.warning("Telegram answerCallbackQuery failed %d: %s", r.status_code, r.text)
