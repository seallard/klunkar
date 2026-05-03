import logging
import sys
from datetime import date
from typing import Annotated

import httpx
import typer

from klunkar import config, db, ranking, release
from klunkar.bot import parse_value_args, run as run_bot
from klunkar.models import Source

app = typer.Typer()
subscribers_app = typer.Typer()
app.add_typer(subscribers_app, name="subscribers")


@app.callback()
def _setup_logging() -> None:
    logging.basicConfig(
        level=config.LOG_LEVEL,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        stream=sys.stderr,
    )


@app.command("migrate")
def migrate() -> None:
    """Run database migrations."""
    with db.get_conn() as conn:
        db.migrate(conn)
    typer.echo("Migrations complete.")


@app.command("check-release")
def check_release() -> None:
    """Check if there is a release tomorrow and notify subscribers."""
    with db.get_conn() as conn:
        with httpx.Client() as client:
            release.prefetch_upcoming(conn, client)
            release.check_and_notify(conn)


def _resolve_date(conn, release_date: str | None) -> date:
    if release_date:
        return date.fromisoformat(release_date)
    upcoming = db.get_upcoming_release_dates(conn, date.today())
    if not upcoming:
        typer.echo("No upcoming release dates cached — run check-release first.")
        raise typer.Exit(1)
    return upcoming[0]


def _resolve_source(name: str) -> Source:
    try:
        return Source(name)
    except ValueError:
        valid = ", ".join(s.value for s in Source)
        typer.echo(f"Unknown source '{name}'. Available: {valid}")
        raise typer.Exit(1)


@app.command("enrich")
def enrich(
    release_date: Annotated[str | None, typer.Option("--date")] = None,
    only: Annotated[str | None, typer.Option("--source", help="Run only this enricher")] = None,
    force: Annotated[bool, typer.Option("--force")] = False,
    scrape_wines: Annotated[
        bool,
        typer.Option("--scrape/--no-scrape", help="Fetch SB wines if missing"),
    ] = True,
) -> None:
    """Run enrichers for a release. Useful for poking at sources between scheduled runs."""
    source = _resolve_source(only) if only else None

    with db.get_conn() as conn:
        target = _resolve_date(conn, release_date)
        with httpx.Client() as client:
            if scrape_wines and not db.has_wines_for(conn, target):
                typer.echo(f"Scraping Systembolaget for {target}…")
                products = release._fetch_with_key_refresh(target, conn, client)
                db.upsert_wines(conn, release._wines_from_products(target, products))
                typer.echo(f"  saved {len(products)} wines")

            summary = release.enrich_release(conn, client, target, only=source, force=force)

    if not summary:
        typer.echo("No enrichers ran (use --force to override the refresh policy).")
        return
    for src, (matched, total) in summary.items():
        typer.echo(f"{src}: {matched}/{total} matched")


@app.command("refetch")
def refetch(
    release_date: Annotated[
        str, typer.Argument(help="Release date (YYYY-MM-DD) to wipe and re-fetch")
    ],
    yes: Annotated[
        bool, typer.Option("--yes", "-y", help="Skip the interactive confirmation")
    ] = False,
) -> None:
    """Wipe a release and re-fetch from scratch (Systembolaget + all enrichers).

    Preserves notified_subscribers and seen_releases — does NOT trigger re-notifications.
    Use this when you want fresh data for a release that already has stale rows.
    """
    target = date.fromisoformat(release_date)
    if not yes:
        typer.confirm(
            f"Wipe and re-fetch {target}? This deletes all wines, enrichments, and "
            "enrichment_runs for that date (notified_subscribers preserved).",
            abort=True,
        )

    with db.get_conn() as conn:
        with httpx.Client() as client:
            wines_deleted, runs_deleted = db.wipe_release(conn, target)
            typer.echo(
                f"Wiped {target}: {wines_deleted} wines (cascade enrichments), "
                f"{runs_deleted} enrichment_runs."
            )

            products = release._fetch_with_key_refresh(target, conn, client)
            if not products:
                typer.echo(f"No products from Systembolaget for {target}; aborting.")
                raise typer.Exit(1)
            db.upsert_wines(conn, release._wines_from_products(target, products))
            typer.echo(f"Scraped {len(products)} wines from Systembolaget.")

            summary = release.enrich_release(conn, client, target)

    if not summary:
        typer.echo("No enrichers ran (unexpected after wipe).")
        return
    for src, (matched, total) in summary.items():
        typer.echo(f"{src}: {matched}/{total} matched")


@app.command("preview")
def preview(
    release_date: Annotated[str | None, typer.Argument()] = None,
    source: Annotated[str, typer.Option("--source")] = "munskankarna",
    value: Annotated[
        str | None, typer.Option("--value", help='Comma-separated; e.g. "fynd,prisvärt"')
    ] = None,
) -> None:
    """Dry run: print the ranked wine list for a release date (default: next upcoming)."""
    src = _resolve_source(source)

    value_set: set[str] | None = None
    if value:
        resolved, unknown = parse_value_args(value)
        if unknown:
            typer.echo(f"Unknown value: {', '.join(unknown)}")
            raise typer.Exit(1)
        value_set = set(resolved) if resolved else None

    with db.get_conn() as conn:
        target = _resolve_date(conn, release_date)
        if not db.has_wines_for(conn, target):
            typer.echo(f"No wines stored for {target} — run enrich/check-release first.")
            raise typer.Exit(1)
        wines = ranking.build_ranked_view(
            conn,
            target,
            source=src,
            value_ratings=value_set,
        )
        if not wines:
            typer.echo(f"No {src}-ranked wines for {target} matching filters.")
            raise typer.Exit(1)
        typer.echo(
            release.format_message(
                wines,
                target,
                source=src,
                value_ratings=value_set,
            )
        )


@app.command("bot")
def bot() -> None:
    """Run the long-polling Telegram bot."""
    run_bot()


@subscribers_app.command("list")
def subscribers_list() -> None:
    """List all subscribers."""
    with db.get_conn() as conn:
        subs = db.get_subscribers(conn)
    for s in subs:
        cats = ",".join(s.value_filter) if s.value_filter else "-"
        typer.echo(f"{s.chat_id}\tbudget={s.max_price}\tsource={s.rank_source}\tvalue={cats}")
    typer.echo(f"Total: {len(subs)}")
