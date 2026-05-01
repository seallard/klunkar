import os

TELEGRAM_BOT_TOKEN: str = os.getenv("TELEGRAM_BOT_TOKEN", "")
DATABASE_URL: str = os.environ.get("DATABASE_URL", "")
TOP_N: int = int(os.getenv("TOP_N", "10"))
LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")
VIVINO_RATING_PRIOR: int = int(os.getenv("VIVINO_RATING_PRIOR", "50"))
ENRICHMENT_REFRESH_HOURS: int = int(os.getenv("ENRICHMENT_REFRESH_HOURS", "24"))
BACKFILL_WINDOW_DAYS: int = int(os.getenv("BACKFILL_WINDOW_DAYS", "14"))
