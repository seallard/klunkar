# klunkar

## Project Summary
Telegram bot that texts subscribers the top 10 wines from Systembolaget's upcoming *tillfälliga sortiment* release, ranked by Vivino score, sent the day before each release.

One Python package, one CLI (`klunkar`), **two processes** running from the same codebase:

- **`klunkar bot`** — long-running Telegram **long polling** (not webhooks). Handles `/start`, `/stop`.
- **`klunkar check-release`** — one-shot, run daily by cron. Prefetches and caches upcoming releases, then fans out to subscribers.

All commands read exclusively from the DB. Only `check-release` makes external HTTP requests.

## Project Structure

```
klunkar/
  cli.py            # typer entrypoints
  config.py         # env-based settings
  db.py             # Postgres: subscribers, seen_releases
  systembolaget.py  # release schedule + product fetch
  vivino.py         # score lookup
  telegram.py       # low-level bot client
  bot.py            # long-polling loop + command handlers
  release.py        # "is there a release tomorrow? notify" job
```

## Tech stack
- Python 3.11+
- httpx — async HTTP client
- pydantic — data validation and models

## Runtime: Railway

- **Service**: `klunkar bot` running continuously
- **Cron service**: `klunkar check-release` once per day
- **Managed Postgres** add-on, shared by both
- Secrets via Railway env vars

## Important Notes
- When the project structure changes (new files, directories, or significant reorganization), update this CLAUDE.md file to reflect the changes.
