# klunkar

Telegram bot that texts subscribers the top 10 wines from Systembolaget's upcoming *tillfälliga sortiment* release, ranked by Vivino score, sent the day before each release.

## Architecture

One Python package, one CLI (`klunkar`), **two processes** running from the same codebase:

- **`klunkar bot`** — long-running. Telegram **long polling** (not webhooks — no HTTPS needed). Handles `/start`, `/stop`.
- **`klunkar check-release`** — one-shot, run daily by cron. If a release is tomorrow: fetch products, score via Vivino, rank, fan out to every subscriber.

No HTTP API. Telegram is the user interface.

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

Deploy with `git push`. Both services run the same CLI; only the start command differs.

Decided, do not re-litigate:
- Railway
- Postgres
- Long polling over webhooks — no public HTTPS endpoint to manage.

## CLI

- `klunkar bot` — service entrypoint
- `klunkar check-release` — cron entrypoint
- `klunkar preview [DATE]` — dry run, print the ranked list, send nothing
- `klunkar subscribers list` — ops

## Config (env)

- `TELEGRAM_BOT_TOKEN`
- `DATABASE_URL` (Railway-provided)
- `TOP_N` — default `10`
- `LOG_LEVEL` — default `INFO`

## Conventions

- **Idempotency**: `check-release` records notified releases in `seen_releases`; re-running the same day must not double-send.
- **Partial failures**: one wine's Vivino lookup failing must not drop the whole batch — log and continue.
- **No persistent Vivino cache yet** — per-run in-memory only.

## Systembolaget integration

No official API. We use the same endpoint systembolaget.se itself calls:

- `GET https://api-extern.systembolaget.se/sb-api-ecommerce/v1/productsearch/search`
- Header: `Ocp-Apim-Subscription-Key: <key>`
- Filter params for our use case: `productLaunch.min`, `productLaunch.max` (both `YYYY-MM-DD`), `assortmentText=Tillfälligt sortiment`, `categoryLevel1=Vin`
- Pagination: 30/page via `page=N`; use `metadata.totalPages`
- The frontend URL names (`saljstart-fran`, etc.) are silently ignored — use the API names above

### The APIM key

The key is a `NEXT_PUBLIC_API_KEY_APIM` value embedded in systembolaget.se's public JS bundle. It's client-side-public but rotatable.

- **Bootstrap**: scrape the key on first run and cache it (env var or DB row).
- **On 401**: re-scrape from a fresh JS bundle. Extraction: fetch any product-listing page, download the referenced `/_next/static/chunks/*.js` files, grep for 32-hex strings near `Ocp-Apim-Subscription-Key`.
- Known-good key as of 2026-04-17: `8d39a7340ee7439f8b4c1e995c8f3e4a` (reference only — prefer runtime scraping).

### Release detection

We do **not** try to discover the next upcoming release date. `check-release` runs daily and asks: *"is there a release tomorrow?"*

```
productLaunch.min = tomorrow
productLaunch.max = tomorrow
assortmentText    = Tillfälligt sortiment
categoryLevel1    = Vin
```

If `metadata.docCount > 0` → fetch all pages, score via Vivino, rank, notify. Otherwise exit quietly. `check-release` checks both today and tomorrow on each run; `seen_releases` ensures no double-sends.

## Vivino integration

No official API. Two-step lookup against the unofficial internal API:

**Step 1 — get winery wines:**
```
GET https://www.vivino.com/api/wineries/<seo_name>/wines
```
- `seo_name` = producer name lowercased, spaces→hyphens (e.g. `cloudy-bay`)
- Returns `wines[]` each with `id`, `name`, `statistics.ratings_average`, `statistics.ratings_count`
- No auth required; plain `User-Agent` header sufficient

**Step 2 — match wine by name:**
- Fuzzy-match Systembolaget's wine name against returned `name` fields
- Pick the best match; use its `statistics.ratings_average` as the score

**Why not the explore endpoint:**
- `GET /api/explore/explore` exists and returns ratings, but its `search_term` parameter is silently ignored — it only respects faceted filters (`country_codes[]`, `wine_type_ids[]`, etc.), making it useless for name-based lookup.

**SEO name normalisation edge cases:** strip accents, drop punctuation (`Château` → `chateau`), handle multi-word names with hyphens.

## Ranking

Bayesian average — avoids a 3-rating 4.9 beating a 900-rating 4.3:

    score = (v / (v + m)) * R + (m / (v + m)) * C

- `R` = wine's `ratings_average`
- `v` = wine's `ratings_count`
- `m` = prior weight (default `50`; tune if the release's count distribution calls for it)
- `C` = mean `ratings_average` across the wines in this release we successfully scored

Wines we couldn't match on Vivino are excluded from the ranking, not zero-scored. If fewer than `TOP_N` remain, send what we have.

## Notification message

One Telegram message per subscriber. Bullet list of the top `TOP_N` wines; each bullet has:

- wine name
- score (one decimal)
- link to the Vivino wine page (from the matched wine's Vivino id)
- link to the Systembolaget product page (from the product's Systembolaget id)
