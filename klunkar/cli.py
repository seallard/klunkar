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
            for offset in (0, 1):
                release.check_and_notify(conn, client, today + timedelta(days=offset))


@app.command("preview")
def preview(release_date: Optional[str] = typer.Argument(None)) -> None:
    """Dry run: print the ranked wine list for a release date (default: today)."""
    _setup_logging()
    from klunkar import db, release

    if release_date:
        target = date.fromisoformat(release_date)
    else:
        target = date.today()

    with db.get_conn() as conn:
        db.migrate(conn)
        with httpx.Client() as client:
            products = release._fetch_with_key_refresh(target, conn, client)
            if not products:
                typer.echo(f"No products found for {target}")
                raise typer.Exit(1)
            wines = release.rank_release(products, client)
            if not wines:
                typer.echo("No wines could be scored.")
                raise typer.Exit(1)
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
