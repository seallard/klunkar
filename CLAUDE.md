# klunkar

Telegram bot that texts subscribers the top 10 wines from Systembolaget's upcoming *tillfälliga sortiment* release, ranked by Vivino score, sent the day before each release.

## Architecture

One Python package, one CLI (`klunkar`), **two processes** running from the same codebase:

- **`klunkar bot`** — long-running Telegram **long polling** (not webhooks). Handles `/start`, `/stop`.
- **`klunkar check-release`** — one-shot, run daily by cron. Prefetches and caches upcoming releases, then fans out to subscribers.

All commands read exclusively from the DB. Only `check-release` makes external HTTP requests.

## Package layout

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

## Runtime: Railway

- **Service**: `klunkar bot` running continuously
- **Cron service**: `klunkar check-release` once per day
- **Managed Postgres** add-on, shared by both
- Secrets via Railway env vars

Deploy with `git push`.

Decided, do not re-litigate:
- Railway
- Postgres
- Long polling over webhooks — no public HTTPS endpoint to manage.

## CLI

- `klunkar bot` — service entrypoint
- `klunkar check-release` — cron entrypoint
- `klunkar preview [DATE]` — print ranked list from DB cache
- `klunkar subscribers list` — ops

## Config (env)

- `TELEGRAM_BOT_TOKEN`
- `DATABASE_URL` (Railway-provided)
- `TOP_N` — default `10`
- `LOG_LEVEL` — default `INFO`

## Conventions

- **Idempotency**: `check-release` records notified releases in `seen_releases`; re-running the same day must not double-send.
- **Partial failures**: one wine's Vivino lookup failing must not drop the whole batch — log and continue.
- **No persistent Vivino cache** — per-run in-memory only.

## Systembolaget integration

- Endpoint: `GET https://api-extern.systembolaget.se/sb-api-ecommerce/v1/productsearch/search`
- Auth header: `Ocp-Apim-Subscription-Key: <key>`
- Filters for tillfälligt sortiment wine: `assortmentText=Tillfälligt sortiment`, `categoryLevel1=Vin`
- The `productLaunchDate.min/max` params are silently ignored by the API — date filtering is done client-side on the `productLaunchDate` field of each returned product.

### The APIM key

The key is `NEXT_PUBLIC_API_KEY_APIM` embedded in Systembolaget's public JS bundle — client-side public but rotatable.

- **Bootstrap**: scrape on first run, cache in DB.
- **On 401**: re-scrape from a fresh JS bundle. Fetch any product-listing page, download `/_next/static/chunks/*.js` files, find the 32-hex string near `Ocp-Apim-Subscription-Key`.
- Known-good key as of 2026-04-17: `8d39a7340ee7439f8b4c1e995c8f3e4a`

### Release detection

`scrape_release_dates` scrapes the official calendar page for tillfälligt sortiment dates only (anchored on `/sortiment/tillfalligt-sortiment/` hrefs). `check-release` checks both today and tomorrow on each run; `seen_releases` ensures no double-sends.

## Vivino integration

Two-step lookup against the unofficial internal API:

1. `GET https://www.vivino.com/api/wineries/<seo_name>/wines` — returns wines with ratings
2. Fuzzy-match Systembolaget's wine name against returned names

**Why not the explore endpoint:** `GET /api/explore/explore` silently ignores `search_term` — it only respects faceted filters, making it useless for name-based lookup.

## Ranking

Bayesian average — avoids a 3-rating 4.9 beating a 900-rating 4.3:

    score = (v / (v + m)) * R + (m / (v + m)) * C

- `R` = wine's `ratings_average`, `v` = `ratings_count`
- `m` = prior weight (default `50`)
- `C` = mean `ratings_average` across scored wines in this release

Wines without a Vivino match are excluded, not zero-scored.
