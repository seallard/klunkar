import logging
import time
from collections.abc import Callable
from datetime import date, timedelta

import httpx
import psycopg

from klunkar import config, db, ranking
from klunkar.models import Source
from klunkar.release import _escape, _source_label, _sv_date, format_message
from klunkar.telegram import answer_callback_query, edit_message_text, send_message

log = logging.getLogger(__name__)

_POLL_TIMEOUT = 30
_WELCOME = (
    "🍷 *Välkommen till Klunkar\\!*\n\n"
    "Dagen innan varje släpp av tillfälligt sortiment på Systembolaget får du de tio "
    "bäst betygsatta vinerna\\.\n\n"
    "Skriv /settings för att se dina inställningar eller /help för alla kommandon\\."
)

_VALUE_CANONICAL = ["fynd", "mer än prisvärt", "prisvärt", "ej prisvärt"]
_VALUE_ALIASES = {
    "fynd": "fynd",
    "mer": "mer än prisvärt",
    "mer än prisvärt": "mer än prisvärt",
    "mer-an-prisvart": "mer än prisvärt",
    "prisv": "prisvärt",
    "prisvärt": "prisvärt",
    "prisvart": "prisvärt",
    "ej": "ej prisvärt",
    "ej prisvärt": "ej prisvärt",
    "ej-prisvart": "ej prisvärt",
}


def parse_category_args(arg: str) -> tuple[list[str], list[str]]:
    """Returns (resolved, unknown). Empty arg returns ([], [])."""
    return _parse_aliased(arg, _VALUE_ALIASES)


# Wine type filter — values match Systembolaget's `categoryLevel2` exactly so
# the DB filter is direct equality. Aliases let the user type "rött" / "red".
_WINE_TYPE_CANONICAL = ["Rött vin", "Vitt vin", "Rosévin", "Mousserande vin"]
_WINE_TYPE_ALIASES = {
    "rött": "Rött vin",
    "rött vin": "Rött vin",
    "rod": "Rött vin",
    "rött-vin": "Rött vin",
    "red": "Rött vin",
    "vitt": "Vitt vin",
    "vitt vin": "Vitt vin",
    "vitt-vin": "Vitt vin",
    "white": "Vitt vin",
    "rosé": "Rosévin",
    "rose": "Rosévin",
    "rosévin": "Rosévin",
    "rosevin": "Rosévin",
    "mousserande": "Mousserande vin",
    "mousserande vin": "Mousserande vin",
    "mousserande-vin": "Mousserande vin",
    "bubbel": "Mousserande vin",
    "sparkling": "Mousserande vin",
}


def parse_wine_type_args(arg: str) -> tuple[list[str], list[str]]:
    """Returns (resolved, unknown). Empty arg returns ([], [])."""
    return _parse_aliased(arg, _WINE_TYPE_ALIASES)


def _parse_aliased(arg: str, aliases: dict[str, str]) -> tuple[list[str], list[str]]:
    raw = arg.strip().lower()
    if not raw:
        return [], []
    tokens = [t.strip() for t in raw.split(",") if t.strip()]
    resolved: list[str] = []
    unknown: list[str] = []
    for tok in tokens:
        canon = aliases.get(tok)
        if canon is None:
            unknown.append(tok)
        elif canon not in resolved:
            resolved.append(canon)
    return resolved, unknown


def _get_updates(base: str, client: httpx.Client, offset: int) -> list[dict]:
    r = client.get(
        f"{base}/getUpdates",
        params={
            "timeout": _POLL_TIMEOUT,
            "offset": offset,
            "allowed_updates": ["message", "callback_query"],
        },
        timeout=_POLL_TIMEOUT + 10,
    )
    r.raise_for_status()
    return r.json().get("result", [])


def _send_ranked(
    chat_id: int, conn: psycopg.Connection, release_date: date, source: Source | str
) -> bool:
    value_filter = db.get_subscriber_value_filter(conn, chat_id)
    value_set = set(value_filter) if value_filter else None
    type_filter = db.get_subscriber_wine_type_filter(conn, chat_id)
    type_set = set(type_filter) if type_filter else None
    country_filter = db.get_subscriber_country_filter(conn, chat_id)
    country_set = set(country_filter) if country_filter else None
    ranked = ranking.build_ranked_view(
        conn,
        release_date,
        source=source,
        value_ratings=value_set,
        wine_types=type_set,
        countries=country_set,
    )
    if not ranked:
        return False
    max_price = db.get_subscriber_budget(conn, chat_id)
    type_counts = db.get_release_type_counts(conn, release_date)
    send_message(
        chat_id,
        format_message(
            ranked,
            release_date,
            source=source,
            max_price=max_price,
            value_ratings=value_set,
            wine_types=type_set,
            countries=country_set,
            type_counts=type_counts,
        ),
    )
    return True


def _resolve_active_date(conn: psycopg.Connection) -> date | None:
    tomorrow = date.today() + timedelta(days=1)
    if db.has_wines_for(conn, tomorrow):
        return tomorrow
    return db.get_last_release_with_data(conn)


def _empty_view_message(target: date) -> str:
    return (
        f"Inga viner för {_sv_date(target)} matchar dina filter. "
        "Se /settings för aktuella inställningar."
    )


def _handle_start(chat_id: int, conn: psycopg.Connection) -> None:
    new = db.add_subscriber(conn, chat_id)
    send_message(chat_id, _WELCOME)
    if new:
        target = _resolve_active_date(conn)
        if target:
            source = db.get_subscriber_rank_source(conn, chat_id)
            _send_ranked(chat_id, conn, target, source)
    log.info("/start from %d (new=%s)", chat_id, new)


def _handle_budget(chat_id: int, text: str, conn: psycopg.Connection) -> None:
    parts = text.split()

    if len(parts) < 2:
        current = db.get_subscriber_budget(conn, chat_id)
        if current is None:
            status = "Ingen budget satt — du får alla tio bästa vinerna."
        else:
            status = f"Aktuell budget: {int(current)} kr."
        send_message(
            chat_id,
            _escape(f"{status}\nSätt med /budget 150. Rensa alla filter med /clear."),
        )
        return

    arg = parts[1].strip().lower()
    try:
        max_price = float(arg)
    except ValueError:
        send_message(chat_id, _escape("Ange ett giltigt belopp, t.ex. /budget 150."))
        return
    db.set_subscriber_budget(conn, chat_id, max_price)
    send_message(chat_id, _escape(f"Budget satt till {int(max_price)} kr."))

    target = db.get_subscriber_preview_date(conn, chat_id) or _resolve_active_date(conn)
    if target:
        source = db.get_subscriber_rank_source(conn, chat_id)
        _send_ranked(chat_id, conn, target, source)
    log.info("/budget from %d", chat_id)


def _source_keyboard(
    current: Source, *, callback_prefix: str = "src", back_data: str | None = None
) -> dict:
    row = []
    for s in Source:
        label = _source_label(s)
        prefix = "✅ " if s is current else ""
        row.append({"text": f"{prefix}{label}", "callback_data": f"{callback_prefix}:{s.value}"})
    rows = [row]
    if back_data:
        rows.append([{"text": "↩ Tillbaka", "callback_data": back_data}])
    return {"inline_keyboard": rows}


def _source_picker_text(current: Source) -> str:
    return f"*Välj rankningskälla*\nAktiv: {_escape(_source_label(current))}"


def _handle_source(chat_id: int, text: str, conn: psycopg.Connection) -> None:
    parts = text.split()
    target = _resolve_active_date(conn)

    if len(parts) < 2:
        current = db.get_subscriber_rank_source(conn, chat_id)
        send_message(chat_id, _source_picker_text(current), reply_markup=_source_keyboard(current))
        return

    raw = parts[1].strip().lower()
    try:
        choice = Source(raw)
    except ValueError:
        valid = ", ".join(s.value for s in Source)
        send_message(chat_id, _escape(f"Okänd källa '{raw}'. Giltiga: {valid}."))
        return

    db.set_subscriber_rank_source(conn, chat_id, choice)
    send_message(chat_id, f"Källa satt till *{_escape(_source_label(choice))}*\\.")

    if target and not _send_ranked(chat_id, conn, target, choice):
        send_message(chat_id, _escape(_empty_view_message(target)))
    log.info("/source %s from %d", choice, chat_id)


def _handle_source_callback(
    chat_id: int, message_id: int, payload: str, conn: psycopg.Connection
) -> None:
    try:
        choice = Source(payload)
    except ValueError:
        log.warning("Unknown source callback payload: %r", payload)
        return
    db.set_subscriber_rank_source(conn, chat_id, choice)
    edit_message_text(
        chat_id,
        message_id,
        _source_picker_text(choice),
        reply_markup=_source_keyboard(choice),
    )
    target = _resolve_active_date(conn)
    if target and not _send_ranked(chat_id, conn, target, choice):
        send_message(chat_id, _escape(_empty_view_message(target)))
    log.info("/source callback %s from %d", choice, chat_id)


# Short tokens for callback_data (≤64 bytes total). Map both directions.
_CATEGORY_TOKENS: dict[str, str] = {
    "fynd": "fynd",
    "mer": "mer än prisvärt",
    "prisv": "prisvärt",
    "ej": "ej prisvärt",
}
_CATEGORY_TOKEN_FROM_CANONICAL = {v: k for k, v in _CATEGORY_TOKENS.items()}


def _category_picker_text(active: list[str]) -> str:
    if active:
        body = f"Aktiv: {_escape(', '.join(active))}"
    else:
        body = _escape("Aktiv: ingen (alla nivåer)")
    return f"*Välj prisvärdhet*\n{body}"


def _category_keyboard(
    active: list[str],
    *,
    callback_prefix: str = "cat",
    done_label: str = "Klar — visa lista",
    done_callback: str | None = None,
) -> dict:
    active_set = set(active)
    rows: list[list[dict]] = []
    pair: list[dict] = []
    for canonical in _VALUE_CANONICAL:
        token = _CATEGORY_TOKEN_FROM_CANONICAL[canonical]
        prefix = "✅" if canonical in active_set else "◯"
        pair.append(
            {"text": f"{prefix} {canonical}", "callback_data": f"{callback_prefix}:{token}"}
        )
        if len(pair) == 2:
            rows.append(pair)
            pair = []
    if pair:
        rows.append(pair)
    rows.append([{"text": done_label, "callback_data": done_callback or f"{callback_prefix}:done"}])
    return {"inline_keyboard": rows}


def _handle_category(chat_id: int, text: str, conn: psycopg.Connection) -> None:
    parts = text.split(maxsplit=1)
    arg = parts[1] if len(parts) >= 2 else ""

    if not arg.strip():
        current = db.get_subscriber_value_filter(conn, chat_id) or []
        send_message(
            chat_id, _category_picker_text(current), reply_markup=_category_keyboard(current)
        )
        return

    resolved, unknown = parse_category_args(arg)
    if unknown:
        send_message(
            chat_id,
            _escape(
                f"Okänd prisvärdhet: {', '.join(unknown)}. Giltiga: {', '.join(_VALUE_CANONICAL)}."
            ),
        )
        return

    if not resolved:
        send_message(chat_id, _escape("Ange minst en prisvärdhet, t.ex. /category fynd."))
        return

    db.set_subscriber_value_filter(conn, chat_id, resolved)
    send_message(
        chat_id,
        _escape(f"Prisvärdhetsfilter satt till: {', '.join(resolved)}."),
    )

    target = db.get_subscriber_preview_date(conn, chat_id) or _resolve_active_date(conn)
    if target:
        source = db.get_subscriber_rank_source(conn, chat_id)
        if not _send_ranked(chat_id, conn, target, source):
            send_message(chat_id, _escape(_empty_view_message(target)))
    log.info("/category from %d → %s", chat_id, resolved or "[cleared]")


def _handle_category_callback(
    chat_id: int, message_id: int, payload: str, conn: psycopg.Connection
) -> None:
    if payload == "done":
        active = db.get_subscriber_value_filter(conn, chat_id) or []
        if active:
            final = f"Prisvärdhet satt till: {_escape(', '.join(active))}"
        else:
            final = _escape("Prisvärdhetsfilter borttaget — alla nivåer visas.")
        edit_message_text(chat_id, message_id, final)

        target = db.get_subscriber_preview_date(conn, chat_id) or _resolve_active_date(conn)
        if target:
            source = db.get_subscriber_rank_source(conn, chat_id)
            if not _send_ranked(chat_id, conn, target, source):
                send_message(chat_id, _escape(_empty_view_message(target)))
        log.info("/category callback done from %d → %s", chat_id, active or "[cleared]")
        return

    canonical = _CATEGORY_TOKENS.get(payload)
    if canonical is None:
        log.warning("Unknown category callback token: %r", payload)
        return

    current = db.get_subscriber_value_filter(conn, chat_id) or []
    if canonical in current:
        new = [c for c in current if c != canonical]
    else:
        new = current + [canonical]
    db.set_subscriber_value_filter(conn, chat_id, new or None)

    edit_message_text(
        chat_id, message_id, _category_picker_text(new), reply_markup=_category_keyboard(new)
    )
    log.info("/category callback toggle %s from %d → %s", canonical, chat_id, new)


_WINE_TYPE_TOKENS: dict[str, str] = {
    "rod": "Rött vin",
    "vit": "Vitt vin",
    "rose": "Rosévin",
    "mou": "Mousserande vin",
}
_WINE_TYPE_TOKEN_FROM_CANONICAL = {v: k for k, v in _WINE_TYPE_TOKENS.items()}


def _winetype_picker_text(active: list[str]) -> str:
    if active:
        body = f"Aktiv: {_escape(', '.join(active))}"
    else:
        body = _escape("Aktiv: ingen (alla vintyper)")
    return f"*Välj vintyp*\n{body}"


def _winetype_keyboard(
    active: list[str],
    *,
    callback_prefix: str = "wt",
    done_label: str = "Klar — visa lista",
    done_callback: str | None = None,
) -> dict:
    active_set = set(active)
    rows: list[list[dict]] = []
    pair: list[dict] = []
    for canonical in _WINE_TYPE_CANONICAL:
        token = _WINE_TYPE_TOKEN_FROM_CANONICAL[canonical]
        prefix = "✅" if canonical in active_set else "◯"
        pair.append(
            {"text": f"{prefix} {canonical}", "callback_data": f"{callback_prefix}:{token}"}
        )
        if len(pair) == 2:
            rows.append(pair)
            pair = []
    if pair:
        rows.append(pair)
    rows.append([{"text": done_label, "callback_data": done_callback or f"{callback_prefix}:done"}])
    return {"inline_keyboard": rows}


def _handle_winetype_callback(
    chat_id: int, message_id: int, payload: str, conn: psycopg.Connection
) -> None:
    if payload == "done":
        active = db.get_subscriber_wine_type_filter(conn, chat_id) or []
        if active:
            final = f"Vintyp satt till: {_escape(', '.join(active))}"
        else:
            final = _escape("Vintypfilter borttaget — alla vintyper visas.")
        edit_message_text(chat_id, message_id, final)

        target = db.get_subscriber_preview_date(conn, chat_id) or _resolve_active_date(conn)
        if target:
            source = db.get_subscriber_rank_source(conn, chat_id)
            if not _send_ranked(chat_id, conn, target, source):
                send_message(chat_id, _escape(_empty_view_message(target)))
        log.info("/winetype callback done from %d → %s", chat_id, active or "[cleared]")
        return

    canonical = _WINE_TYPE_TOKENS.get(payload)
    if canonical is None:
        log.warning("Unknown winetype callback token: %r", payload)
        return

    current = db.get_subscriber_wine_type_filter(conn, chat_id) or []
    if canonical in current:
        new = [c for c in current if c != canonical]
    else:
        new = current + [canonical]
    db.set_subscriber_wine_type_filter(conn, chat_id, new or None)

    edit_message_text(
        chat_id, message_id, _winetype_picker_text(new), reply_markup=_winetype_keyboard(new)
    )
    log.info("/winetype callback toggle %s from %d → %s", canonical, chat_id, new)


def _handle_hub_callback(
    chat_id: int, message_id: int, payload: str, conn: psycopg.Connection
) -> None:
    """Dispatch hub:* callbacks. Payload is everything after `hub:`."""
    sub, _, rest = payload.partition(":")

    if sub == "open" or payload == "":
        edit_message_text(
            chat_id, message_id, _hub_text(conn, chat_id), reply_markup=_hub_keyboard()
        )
        return

    if sub == "clear":
        db.set_subscriber_budget(conn, chat_id, None)
        db.set_subscriber_value_filter(conn, chat_id, None)
        db.set_subscriber_wine_type_filter(conn, chat_id, None)
        db.set_subscriber_country_filter(conn, chat_id, None)
        edit_message_text(
            chat_id, message_id, _hub_text(conn, chat_id), reply_markup=_hub_keyboard()
        )
        log.info("hub:clear from %d", chat_id)
        return

    if sub == "src":
        if not rest:  # open source picker
            current = db.get_subscriber_rank_source(conn, chat_id)
            edit_message_text(
                chat_id,
                message_id,
                _source_picker_text(current),
                reply_markup=_source_keyboard(
                    current, callback_prefix="hub:src", back_data="hub:open"
                ),
            )
            return
        try:
            choice = Source(rest)
        except ValueError:
            log.warning("hub:src unknown value: %r", rest)
            return
        db.set_subscriber_rank_source(conn, chat_id, choice)
        edit_message_text(
            chat_id, message_id, _hub_text(conn, chat_id), reply_markup=_hub_keyboard()
        )
        log.info("hub:src %s from %d", choice, chat_id)
        return

    if sub == "wt":
        if not rest:  # open
            active = db.get_subscriber_wine_type_filter(conn, chat_id) or []
            edit_message_text(
                chat_id,
                message_id,
                _winetype_picker_text(active),
                reply_markup=_winetype_keyboard(
                    active,
                    callback_prefix="hub:wt",
                    done_label="↩ Klar",
                    done_callback="hub:open",
                ),
            )
            return
        canonical = _WINE_TYPE_TOKENS.get(rest)
        if canonical is None:
            log.warning("hub:wt unknown token: %r", rest)
            return
        current = db.get_subscriber_wine_type_filter(conn, chat_id) or []
        new = (
            [c for c in current if c != canonical]
            if canonical in current
            else current + [canonical]
        )
        db.set_subscriber_wine_type_filter(conn, chat_id, new or None)
        edit_message_text(
            chat_id,
            message_id,
            _winetype_picker_text(new),
            reply_markup=_winetype_keyboard(
                new, callback_prefix="hub:wt", done_label="↩ Klar", done_callback="hub:open"
            ),
        )
        log.info("hub:wt toggle %s from %d → %s", canonical, chat_id, new)
        return

    if sub == "cat":
        if not rest:  # open
            active = db.get_subscriber_value_filter(conn, chat_id) or []
            edit_message_text(
                chat_id,
                message_id,
                _category_picker_text(active),
                reply_markup=_category_keyboard(
                    active,
                    callback_prefix="hub:cat",
                    done_label="↩ Klar",
                    done_callback="hub:open",
                ),
            )
            return
        canonical = _CATEGORY_TOKENS.get(rest)
        if canonical is None:
            log.warning("hub:cat unknown token: %r", rest)
            return
        current = db.get_subscriber_value_filter(conn, chat_id) or []
        new = (
            [c for c in current if c != canonical]
            if canonical in current
            else current + [canonical]
        )
        db.set_subscriber_value_filter(conn, chat_id, new or None)
        edit_message_text(
            chat_id,
            message_id,
            _category_picker_text(new),
            reply_markup=_category_keyboard(
                new, callback_prefix="hub:cat", done_label="↩ Klar", done_callback="hub:open"
            ),
        )
        log.info("hub:cat toggle %s from %d → %s", canonical, chat_id, new)
        return

    if sub == "cnt":
        available = _resolve_release_countries(conn)
        if not rest:  # open
            active = db.get_subscriber_country_filter(conn, chat_id) or []
            if not available:
                edit_message_text(
                    chat_id,
                    message_id,
                    _escape("Inga land-data tillgängliga ännu."),
                    reply_markup={
                        "inline_keyboard": [[{"text": "↩ Tillbaka", "callback_data": "hub:open"}]]
                    },
                )
                return
            edit_message_text(
                chat_id,
                message_id,
                _country_picker_text(active),
                reply_markup=_country_keyboard(
                    active,
                    available,
                    callback_prefix="hub:cnt",
                    done_label="↩ Klar",
                    done_callback="hub:open",
                ),
            )
            return
        if rest not in available:
            log.warning("hub:cnt unknown country: %r", rest)
            return
        current = db.get_subscriber_country_filter(conn, chat_id) or []
        new = [c for c in current if c != rest] if rest in current else current + [rest]
        db.set_subscriber_country_filter(conn, chat_id, new or None)
        edit_message_text(
            chat_id,
            message_id,
            _country_picker_text(new),
            reply_markup=_country_keyboard(
                new,
                available,
                callback_prefix="hub:cnt",
                done_label="↩ Klar",
                done_callback="hub:open",
            ),
        )
        log.info("hub:cnt toggle %s from %d → %s", rest, chat_id, new)
        return

    if sub == "bud":
        if not rest:  # open chips
            current = db.get_subscriber_budget(conn, chat_id)
            edit_message_text(
                chat_id,
                message_id,
                _budget_picker_text(current),
                reply_markup=_budget_chips_keyboard(current),
            )
            return
        if rest == "custom":
            send_message(
                chat_id,
                _escape(f"{_BUDGET_PROMPT_PREFIX} (t.ex. 175):"),
                reply_markup={"force_reply": True, "selective": True},
            )
            log.info("hub:bud custom prompt from %d", chat_id)
            return
        if rest == "none":
            db.set_subscriber_budget(conn, chat_id, None)
        else:
            try:
                amount = float(rest)
            except ValueError:
                log.warning("hub:bud unknown value: %r", rest)
                return
            db.set_subscriber_budget(conn, chat_id, amount)
        edit_message_text(
            chat_id, message_id, _hub_text(conn, chat_id), reply_markup=_hub_keyboard()
        )
        log.info("hub:bud %s from %d", rest, chat_id)
        return

    log.warning("Unknown hub subcommand: %r", sub)


def _country_picker_text(active: list[str]) -> str:
    if active:
        body = f"Aktiv: {_escape(', '.join(active))}"
    else:
        body = _escape("Aktiv: ingen (alla länder)")
    return f"*Välj land*\n{body}"


def _country_keyboard(
    active: list[str],
    available: list[str],
    *,
    callback_prefix: str = "cnt",
    done_label: str = "Klar — visa lista",
    done_callback: str | None = None,
) -> dict:
    active_set = set(active)
    rows: list[list[dict]] = []
    pair: list[dict] = []
    for country in available:
        prefix = "✅" if country in active_set else "◯"
        pair.append(
            {"text": f"{prefix} {country}", "callback_data": f"{callback_prefix}:{country}"}
        )
        if len(pair) == 2:
            rows.append(pair)
            pair = []
    if pair:
        rows.append(pair)
    rows.append([{"text": done_label, "callback_data": done_callback or f"{callback_prefix}:done"}])
    return {"inline_keyboard": rows}


def _resolve_release_countries(conn: psycopg.Connection) -> list[str]:
    target = _resolve_active_date(conn)
    return db.get_release_countries(conn, target) if target else []


def _handle_country(chat_id: int, text: str, conn: psycopg.Connection) -> None:
    parts = text.split(maxsplit=1)
    arg = parts[1] if len(parts) >= 2 else ""
    available = _resolve_release_countries(conn)

    if not arg.strip():
        active = db.get_subscriber_country_filter(conn, chat_id) or []
        if not available:
            send_message(chat_id, _escape("Inga land-data tillgängliga ännu."))
            return
        send_message(
            chat_id,
            _country_picker_text(active),
            reply_markup=_country_keyboard(active, available),
        )
        return

    # Text path: case-insensitive match against the active release's countries.
    by_lower = {c.lower(): c for c in available}
    tokens = [t.strip() for t in arg.split(",") if t.strip()]
    resolved: list[str] = []
    unknown: list[str] = []
    for tok in tokens:
        canon = by_lower.get(tok.lower())
        if canon is None:
            unknown.append(tok)
        elif canon not in resolved:
            resolved.append(canon)

    if unknown:
        send_message(
            chat_id,
            _escape(
                f"Okänt land: {', '.join(unknown)}. "
                f"Tillgängliga: {', '.join(available) or '(inga)'}."
            ),
        )
        return
    if not resolved:
        send_message(chat_id, _escape("Ange minst ett land, t.ex. /country Italien."))
        return

    db.set_subscriber_country_filter(conn, chat_id, resolved)
    send_message(chat_id, _escape(f"Landfilter satt till: {', '.join(resolved)}."))

    target = db.get_subscriber_preview_date(conn, chat_id) or _resolve_active_date(conn)
    if target:
        source = db.get_subscriber_rank_source(conn, chat_id)
        if not _send_ranked(chat_id, conn, target, source):
            send_message(chat_id, _escape(_empty_view_message(target)))
    log.info("/country from %d → %s", chat_id, resolved)


def _handle_country_callback(
    chat_id: int, message_id: int, payload: str, conn: psycopg.Connection
) -> None:
    if payload == "done":
        active = db.get_subscriber_country_filter(conn, chat_id) or []
        if active:
            final = f"Land satt till: {_escape(', '.join(active))}"
        else:
            final = _escape("Landfilter borttaget — alla länder visas.")
        edit_message_text(chat_id, message_id, final)

        target = db.get_subscriber_preview_date(conn, chat_id) or _resolve_active_date(conn)
        if target:
            source = db.get_subscriber_rank_source(conn, chat_id)
            if not _send_ranked(chat_id, conn, target, source):
                send_message(chat_id, _escape(_empty_view_message(target)))
        log.info("/country callback done from %d → %s", chat_id, active or "[cleared]")
        return

    available = _resolve_release_countries(conn)
    if payload not in available:
        log.warning("country callback unknown country: %r (available=%r)", payload, available)
        return

    current = db.get_subscriber_country_filter(conn, chat_id) or []
    new = [c for c in current if c != payload] if payload in current else current + [payload]
    db.set_subscriber_country_filter(conn, chat_id, new or None)

    edit_message_text(
        chat_id,
        message_id,
        _country_picker_text(new),
        reply_markup=_country_keyboard(new, available),
    )
    log.info("country callback toggle %s from %d → %s", payload, chat_id, new)


def _handle_winetype(chat_id: int, text: str, conn: psycopg.Connection) -> None:
    parts = text.split(maxsplit=1)
    arg = parts[1] if len(parts) >= 2 else ""

    if not arg.strip():
        current = db.get_subscriber_wine_type_filter(conn, chat_id) or []
        send_message(
            chat_id, _winetype_picker_text(current), reply_markup=_winetype_keyboard(current)
        )
        return

    resolved, unknown = parse_wine_type_args(arg)
    if unknown:
        send_message(
            chat_id,
            _escape(
                f"Okänd vintyp: {', '.join(unknown)}. Giltiga: {', '.join(_WINE_TYPE_CANONICAL)}."
            ),
        )
        return

    if not resolved:
        send_message(chat_id, _escape("Ange minst en vintyp, t.ex. /winetype rött."))
        return

    db.set_subscriber_wine_type_filter(conn, chat_id, resolved)
    send_message(
        chat_id,
        _escape(f"Vintypfilter satt till: {', '.join(resolved)}."),
    )

    target = db.get_subscriber_preview_date(conn, chat_id) or _resolve_active_date(conn)
    if target:
        source = db.get_subscriber_rank_source(conn, chat_id)
        if not _send_ranked(chat_id, conn, target, source):
            send_message(chat_id, _escape(_empty_view_message(target)))
    log.info("/winetype from %d → %s", chat_id, resolved or "[cleared]")


def _handle_clear(chat_id: int, conn: psycopg.Connection) -> None:
    budget = db.get_subscriber_budget(conn, chat_id)
    value_filter = db.get_subscriber_value_filter(conn, chat_id)
    wine_type_filter = db.get_subscriber_wine_type_filter(conn, chat_id)
    country_filter = db.get_subscriber_country_filter(conn, chat_id)
    source = db.get_subscriber_rank_source(conn, chat_id)

    cleared: list[str] = []
    if budget is not None:
        db.set_subscriber_budget(conn, chat_id, None)
        cleared.append("budget")
    if value_filter:
        db.set_subscriber_value_filter(conn, chat_id, None)
        cleared.append("prisvärdhet")
    if wine_type_filter:
        db.set_subscriber_wine_type_filter(conn, chat_id, None)
        cleared.append("vintyp")
    if country_filter:
        db.set_subscriber_country_filter(conn, chat_id, None)
        cleared.append("land")

    if not cleared:
        send_message(chat_id, _escape("Inga filter att rensa."))
    else:
        send_message(
            chat_id,
            _escape(f"Filter rensade: {', '.join(cleared)}.\nKälla kvar: {_source_label(source)}."),
        )

        target = db.get_subscriber_preview_date(conn, chat_id) or _resolve_active_date(conn)
        if target and not _send_ranked(chat_id, conn, target, source):
            send_message(chat_id, _escape(_empty_view_message(target)))
    log.info("/clear from %d → %s", chat_id, cleared or "[noop]")


def _send_for_date(chat_id: int, target: date, conn: psycopg.Connection) -> None:
    if not db.has_wines_for(conn, target):
        if target < date.today():
            msg = f"Inget släpp för {_sv_date(target)}. Se /releases för tillgängliga datum."
        else:
            msg = f"Inga viner finns ännu för {_sv_date(target)}. Försök igen senare."
        send_message(chat_id, _escape(msg))
        return

    db.set_subscriber_preview_date(conn, chat_id, target)
    source = db.get_subscriber_rank_source(conn, chat_id)
    if not _send_ranked(chat_id, conn, target, source):
        send_message(chat_id, _escape(_empty_view_message(target)))


def _handle_next(chat_id: int, conn: psycopg.Connection) -> None:
    upcoming = db.get_upcoming_release_dates(conn, date.today())
    if not upcoming:
        send_message(chat_id, "Inga kommande släpp hittades inom de närmaste 90 dagarna\\.")
        return
    _send_for_date(chat_id, upcoming[0], conn)
    log.info("/next %s from %d", upcoming[0], chat_id)


def _old_picker_keyboard(dates: list[date]) -> dict:
    rows = [[{"text": _sv_date(d), "callback_data": f"old:{d.isoformat()}"}] for d in dates]
    return {"inline_keyboard": rows}


def _handle_old(chat_id: int, text: str, conn: psycopg.Connection) -> None:
    parts = text.split()
    if len(parts) < 2:
        past = list(reversed(db.get_past_release_dates_with_data(conn, since=date.min)))
        if not past:
            send_message(chat_id, _escape("Inga tidigare släpp finns ännu."))
            return
        send_message(chat_id, "*Välj tidigare släpp*", reply_markup=_old_picker_keyboard(past))
        return
    try:
        target = date.fromisoformat(parts[1])
    except ValueError:
        send_message(
            chat_id,
            "Ogiltigt datum\\. Använd format YYYY\\-MM\\-DD, t\\.ex\\. /old 2026\\-04\\-24\\.",
        )
        return
    _send_for_date(chat_id, target, conn)
    log.info("/old %s from %d", target, chat_id)


def _handle_old_callback(
    chat_id: int, message_id: int, payload: str, conn: psycopg.Connection
) -> None:
    try:
        target = date.fromisoformat(payload)
    except ValueError:
        log.warning("old callback bad date: %r", payload)
        return
    edit_message_text(chat_id, message_id, _escape(f"✓ Visar {_sv_date(target)}"))
    _send_for_date(chat_id, target, conn)
    log.info("old callback %s from %d", target, chat_id)


def _handle_recent(chat_id: int, conn: psycopg.Connection) -> None:
    target = db.get_last_release_with_data(conn)
    if target is None:
        send_message(chat_id, "Inga tidigare släpp hittades\\.")
        return

    source = db.get_subscriber_rank_source(conn, chat_id)
    if not _send_ranked(chat_id, conn, target, source):
        send_message(chat_id, _escape(_empty_view_message(target)))
    log.info("/recent %s from %d", target, chat_id)


def _releases_keyboard(upcoming: list[date], past: list[date]) -> dict:
    rows: list[list[dict]] = []
    if upcoming:
        rows.append([{"text": "— Kommande —", "callback_data": "noop"}])
        for d in upcoming:
            rows.append([{"text": _sv_date(d), "callback_data": f"old:{d.isoformat()}"}])
    if past:
        rows.append([{"text": "— Tidigare —", "callback_data": "noop"}])
        for d in past:
            rows.append([{"text": _sv_date(d), "callback_data": f"old:{d.isoformat()}"}])
    return {"inline_keyboard": rows}


def _handle_releases(chat_id: int, conn: psycopg.Connection) -> None:
    upcoming = db.get_upcoming_release_dates(conn, date.today())
    past = list(reversed(db.get_past_release_dates_with_data(conn, since=date.min)))

    if not upcoming and not past:
        send_message(chat_id, "Inga släpp hittades\\.")
        log.info("/releases from %d (empty)", chat_id)
        return

    send_message(
        chat_id,
        "*Tillgängliga släpp*",
        reply_markup=_releases_keyboard(upcoming, past),
    )
    log.info("/releases from %d (upcoming=%d, past=%d)", chat_id, len(upcoming), len(past))


def _hub_text(conn: psycopg.Connection, chat_id: int) -> str:
    source = db.get_subscriber_rank_source(conn, chat_id)
    budget = db.get_subscriber_budget(conn, chat_id)
    value_filter = db.get_subscriber_value_filter(conn, chat_id)
    wine_type_filter = db.get_subscriber_wine_type_filter(conn, chat_id)
    country_filter = db.get_subscriber_country_filter(conn, chat_id)

    next_release: date | None = None
    upcoming = db.get_upcoming_release_dates(conn, date.today())
    if upcoming:
        next_release = upcoming[0]

    budget_text = f"{int(budget)} kr" if budget is not None else "ingen"
    category_text = ", ".join(value_filter) if value_filter else "alla"
    type_text = ", ".join(wine_type_filter) if wine_type_filter else "alla"
    country_text = ", ".join(country_filter) if country_filter else "alla"
    next_text = _sv_date(next_release) if next_release else "okänt"

    return "\n".join(
        [
            "🍷 *Dina inställningar*",
            "",
            f"*Källa:* {_escape(_source_label(source))}",
            f"*Budget:* {_escape(budget_text)}",
            f"*Vintyp:* {_escape(type_text)}",
            f"*Land:* {_escape(country_text)}",
            f"*Prisvärdhet:* {_escape(category_text)}",
            "",
            f"*Nästa släpp:* {_escape(next_text)}",
        ]
    )


def _hub_keyboard() -> dict:
    return {
        "inline_keyboard": [
            [
                {"text": "Ändra källa", "callback_data": "hub:src"},
                {"text": "Ändra vintyp", "callback_data": "hub:wt"},
            ],
            [
                {"text": "Ändra prisvärdhet", "callback_data": "hub:cat"},
                {"text": "Ändra budget", "callback_data": "hub:bud"},
            ],
            [
                {"text": "Ändra land", "callback_data": "hub:cnt"},
            ],
            [
                {"text": "🧹 Rensa alla filter", "callback_data": "hub:clear"},
            ],
        ]
    }


_BUDGET_CHIPS: list[int | None] = [150, 250, 500, None]


_BUDGET_PROMPT_PREFIX = "💰 Skriv din budget i kr"


def _budget_chips_keyboard(current: float | None) -> dict:
    rows: list[list[dict]] = []
    pair: list[dict] = []
    for chip in _BUDGET_CHIPS:
        if chip is None:
            label = "Ingen"
            payload = "none"
            active = current is None
        else:
            label = f"{chip} kr"
            payload = str(chip)
            active = current is not None and int(current) == chip
        prefix = "✅ " if active else ""
        pair.append({"text": f"{prefix}{label}", "callback_data": f"hub:bud:{payload}"})
        if len(pair) == 2:
            rows.append(pair)
            pair = []
    if pair:
        rows.append(pair)
    rows.append([{"text": "✏️ Annat belopp", "callback_data": "hub:bud:custom"}])
    rows.append([{"text": "↩ Tillbaka", "callback_data": "hub:open"}])
    return {"inline_keyboard": rows}


def _budget_picker_text(current: float | None) -> str:
    body = f"Aktiv: {int(current)} kr" if current is not None else _escape("Aktiv: ingen")
    return f"*Välj budget*\n{body}"


def _handle_settings(chat_id: int, conn: psycopg.Connection) -> None:
    send_message(chat_id, _hub_text(conn, chat_id), reply_markup=_hub_keyboard())
    log.info("/settings from %d", chat_id)


def _handle_help(chat_id: int) -> None:
    send_message(
        chat_id,
        "🍷 *Klunkar* — bästa vinerna från Systembolagets tillfälliga sortiment\\.\n\n"
        "*Listor*\n"
        "/next — nästa släpp\n"
        "/recent — senaste släpp\n"
        "/old — välj tidigare släpp\n"
        "/releases — alla tillgängliga släpp\n\n"
        "*Filter*\n"
        "/settings — knappar för alla filter\n"
        "/source · /budget 150 · /winetype rött · /country Italien · /category fynd · /clear\n\n"
        "/start · /stop · /help",
    )
    log.info("/help from %d", chat_id)


def _handle_stop(chat_id: int, conn: psycopg.Connection) -> None:
    removed = db.remove_subscriber(conn, chat_id)
    send_message(chat_id, "Du är nu avprenumererad\\. Skriv /start för att prenumerera igen\\.")
    log.info("/stop from %d (removed=%s)", chat_id, removed)


_HANDLERS: dict[str, Callable[[int, str, psycopg.Connection], None]] = {
    "/start": lambda c, t, conn: _handle_start(c, conn),
    "/budget": _handle_budget,
    "/source": _handle_source,
    "/category": _handle_category,
    "/winetype": _handle_winetype,
    "/country": _handle_country,
    "/clear": lambda c, t, conn: _handle_clear(c, conn),
    "/next": lambda c, t, conn: _handle_next(c, conn),
    "/old": _handle_old,
    "/recent": lambda c, t, conn: _handle_recent(c, conn),
    "/releases": lambda c, t, conn: _handle_releases(c, conn),
    "/settings": lambda c, t, conn: _handle_settings(c, conn),
    "/help": lambda c, t, conn: _handle_help(c),
    "/stop": lambda c, t, conn: _handle_stop(c, conn),
}


def _handle_update(update: dict, conn: psycopg.Connection) -> None:
    if "callback_query" in update:
        _handle_callback_query(update["callback_query"], conn)
        return
    msg = update.get("message", {})
    text = msg.get("text", "")
    chat_id = msg.get("chat", {}).get("id")
    if not chat_id or not text:
        return

    # Reply to a force_reply prompt? Route based on the prompt's text.
    reply_to = (msg.get("reply_to_message") or {}).get("text", "")
    if reply_to.startswith(_BUDGET_PROMPT_PREFIX):
        _handle_custom_budget_reply(chat_id, text, conn)
        return

    cmd = text.split(maxsplit=1)[0]
    handler = _HANDLERS.get(cmd)
    if handler is not None:
        handler(chat_id, text, conn)


def _handle_custom_budget_reply(chat_id: int, text: str, conn: psycopg.Connection) -> None:
    try:
        amount = float(text.strip().replace(",", "."))
    except ValueError:
        send_message(chat_id, _escape("Ogiltigt belopp. Försök igen via /settings."))
        return
    if amount < 0:
        send_message(chat_id, _escape("Belopp måste vara positivt."))
        return
    db.set_subscriber_budget(conn, chat_id, amount)
    send_message(chat_id, _escape(f"Budget satt till {int(amount)} kr."))
    send_message(chat_id, _hub_text(conn, chat_id), reply_markup=_hub_keyboard())
    log.info("custom budget %s from %d", amount, chat_id)


def _handle_callback_query(query: dict, conn: psycopg.Connection) -> None:
    query_id = query.get("id")
    data = query.get("data", "")
    msg = query.get("message", {})
    chat_id = msg.get("chat", {}).get("id")
    message_id = msg.get("message_id")

    if not chat_id or not message_id or ":" not in data:
        if query_id:
            answer_callback_query(query_id)
        return

    prefix, _, payload = data.partition(":")
    try:
        if prefix == "src":
            _handle_source_callback(chat_id, message_id, payload, conn)
        elif prefix == "cat":
            _handle_category_callback(chat_id, message_id, payload, conn)
        elif prefix == "wt":
            _handle_winetype_callback(chat_id, message_id, payload, conn)
        elif prefix == "cnt":
            _handle_country_callback(chat_id, message_id, payload, conn)
        elif prefix == "hub":
            _handle_hub_callback(chat_id, message_id, payload, conn)
        elif prefix == "old":
            _handle_old_callback(chat_id, message_id, payload, conn)
        else:
            log.warning("Unknown callback prefix: %r", prefix)
    finally:
        if query_id:
            answer_callback_query(query_id)


def run() -> None:
    if not config.TELEGRAM_BOT_TOKEN:
        raise RuntimeError("TELEGRAM_BOT_TOKEN is not set")

    base = f"https://api.telegram.org/bot{config.TELEGRAM_BOT_TOKEN}"
    offset = 0

    with db.get_conn() as conn:
        log.info("Bot started, long-polling…")
        with httpx.Client() as client:
            while True:
                try:
                    updates = _get_updates(base, client, offset)
                except Exception as e:
                    log.warning("Poll error: %s — retrying in 5s", e)
                    time.sleep(5)
                    continue

                for update in updates:
                    try:
                        _handle_update(update, conn)
                    except Exception as e:
                        log.error("Error handling update %s: %s", update.get("update_id"), e)
                        conn.rollback()
                    offset = update["update_id"] + 1
