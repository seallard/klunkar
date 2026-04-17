import httpx

from klunkar import config


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
        r.raise_for_status()
