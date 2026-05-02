# klunkar

## Project Summary
Telegram bot that texts subscribers the top 10 wines from Systembolaget's upcoming *tillfälliga sortiment* release. Each subscriber picks which source ranks their list — currently Munskänkarna (default) or Vivino — sent the day before each release (and again when a late-publishing source like Munskänkarna lands).

One Python package, one CLI (`klunkar`), **two processes** running from the same codebase:

- **`klunkar bot`** — long-running Telegram **long polling** (not webhooks). Handles `/start`, `/stop`, `/budget`, `/source`, `/category`, `/clear`, `/next`, `/recent`, `/old`, `/releases`, `/settings`, `/help`.
- **`klunkar check-release`** — one-shot, run daily by cron. Prefetches all SB wines for upcoming releases, runs every enricher, fans out to subscribers (per-`(release_date, chat_id)` dedup), then **back-fills enrichment + retroactive notifications for recent past releases** within `BACKFILL_WINDOW_DAYS` (default 14). This catches Munskänkarna-ranked subscribers who were skipped pre-release because Munskänkarna's review page hadn't published yet — once it lands, the next cron tick enriches the past release and sends the message.

All commands read exclusively from the DB. Only `check-release` and `enrich` make external HTTP requests.

## Architecture

The Systembolaget product list is the **spine**. Every release wine is persisted independent of any external lookup. **Enrichers** (one per data source) attach per-source payloads (Vivino score, Munskänkarna review, …) to wines via the `wine_enrichments` table, keyed by `(release_date, sb_product_number, source)` with JSONB payloads.

Ranking is a **query-time projection** keyed on a single source per query — no score merging across sources. Each subscriber stores their preferred `rank_source`. To add a new source: drop a file in `klunkar/sources/` and add it to the `ENRICHERS` registry in `klunkar/sources/__init__.py`.

## Project Structure

```
klunkar/
  cli.py             # typer entrypoints (migrate, check-release, enrich, preview, bot, subscribers)
  config.py          # env-based settings (incl. ENRICHMENT_REFRESH_HOURS)
  models.py          # pydantic: Wine, RankedWine, source payloads
  db.py              # Postgres schema + helpers (wines, wine_enrichments, enrichment_runs, …)
  ranking.py         # per-source build_ranked_view (Bayesian for vivino, raw for munskankarna)
  systembolaget.py   # release calendar + product fetch (the spine)
  vivino.py          # legacy Vivino client (lookup, slugify) — used by sources/vivino.py
  telegram.py        # low-level bot client
  bot.py             # long-polling loop + command handlers
  release.py         # prefetch_upcoming, check_and_notify, format_message
  sources/
    __init__.py      # ENRICHERS registry
    base.py          # Enricher Protocol + EnrichmentResult
    vivino.py        # VivinoEnricher
    munskankarna.py  # MunskankarnaEnricher (HTML scrape of vinlocus release page)
tests/               # pytest; no DB integration (pure-function tests only)
```

## Database

Schema is recreated by `klunkar migrate` (idempotent). Key tables:
- `wines (release_date, sb_product_number, …)` — every SB wine in a release
- `wine_enrichments (release_date, sb_product_number, source, confidence, payload JSONB)` — per-source data
- `enrichment_runs (release_date, source, run_at, matched_count, total_count)` — for refresh policy
- `notified_subscribers (release_date, chat_id)` — per-(date, chat) send dedup; allows a Munskänkarna subscriber to be notified later when the source publishes
- `subscribers.rank_source` — which source's ranking the user wants
- `subscribers.value_filter TEXT[]` — Munskänkarna value-rating filter (e.g. `{fynd}`); NULL = all categories
- `subscribers.created_at` — used by `get_subscribers_to_notify_for` to gate retroactive sends to subscribers who were active at the original notification window (recent joiners get the welcome message instead)

## Tech stack
- Python 3.11+
- httpx — async HTTP client
- pydantic — used for **all internal data shapes** (Wine, Subscriber, source payloads,
  parse intermediates like SBProduct / VivinoMatch). All value models are frozen
  (`ConfigDict(frozen=True)`).
- `klunkar.models.Source` — `StrEnum` for source identifiers; preferred over
  bare strings everywhere. `StrEnum` preserves wire format, so DB rows stay plain text.

## Runtime: Railway

- **Service**: `klunkar bot` running continuously
- **Cron service**: `klunkar check-release` once per day
- **Managed Postgres** add-on, shared by both
- Secrets via Railway env vars

## Important Notes
- When the project structure changes (new files, directories, or significant reorganization), update this CLAUDE.md file to reflect the changes.
