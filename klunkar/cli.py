import logging
import sys
from datetime import date, timedelta
from typing import Optional

import httpx
import typer

from klunkar import config

app = typer.Typer()
subscribers_app = typer.Typer()
app.add_typer(subscribers_app, name="subscribers")


def _setup_logging() -> None:
    logging.basicConfig(
        level=config.LOG_LEVEL,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        stream=sys.stderr,
    )


@app.command("check-release")
def check_release() -> None:
    """Check if there is a release today or tomorrow and notify subscribers."""
    _setup_logging()
    from klunkar import db, release

    today = date.today()
    with db.get_conn() as conn:
        db.migrate(conn)
        with httpx.Client() as client:
            release.prefetch_upcoming(conn, client)
            for offset in (0, 1):
                release.check_and_notify(conn, client, today + timedelta(days=offset))


@app.command("preview")
def preview(release_date: Optional[str] = typer.Argument(None)) -> None:
    """Dry run: print the ranked wine list for a release date (default: next upcoming)."""
    _setup_logging()
    from klunkar import db, release

    with db.get_conn() as conn:
        db.migrate(conn)
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
    _setup_logging()
    from klunkar.bot import run

    run()


@subscribers_app.command("list")
def subscribers_list() -> None:
    """List all subscribers."""
    from klunkar import db

    with db.get_conn() as conn:
        subs = db.get_subscribers(conn)
    for chat_id in subs:
        typer.echo(chat_id)
    typer.echo(f"Total: {len(subs)}")
