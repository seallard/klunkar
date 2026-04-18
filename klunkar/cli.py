import logging
import sys
from datetime import date

import httpx
import typer

from klunkar import config, db, release
from klunkar.bot import run as run_bot

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


@app.command("preview")
def preview(release_date: str | None = typer.Argument(None)) -> None:
    """Dry run: print the ranked wine list for a release date (default: next upcoming)."""

    with db.get_conn() as conn:
        if release_date:
            target = date.fromisoformat(release_date)
        else:
            upcoming = db.get_upcoming_release_dates(conn, date.today())
            if not upcoming:
                typer.echo("No upcoming release dates cached — run check-release first.")
                raise typer.Exit(1)
            target = upcoming[0]
        cached = db.get_release_wines(conn, target)
        if not cached:
            typer.echo(f"No cached wines for {target} — run check-release first.")
            raise typer.Exit(1)
        wines = [
            release.RankedWine(
                rank=r[0], name=r[1], score=r[2], vivino_url=r[3],
                sb_url=r[4], price=r[5] or 0.0, wine_type=r[6] or "",
            )
            for r in cached
        ]
        typer.echo(release.format_message(wines, target))


@app.command("bot")
def bot() -> None:
    """Run the long-polling Telegram bot."""

    run_bot()


@subscribers_app.command("list")
def subscribers_list() -> None:
    """List all subscribers."""
    with db.get_conn() as conn:
        subs = db.get_subscribers(conn)
    for chat_id in subs:
        typer.echo(chat_id)
    typer.echo(f"Total: {len(subs)}")
