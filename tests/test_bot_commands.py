"""Tests the bot command handlers' control flow.

DB and Telegram are mocked; we assert on the messages sent and the DB
state changes triggered by each branch.
"""

from datetime import date
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from klunkar import bot


@pytest.fixture
def fake_state(monkeypatch):
    """Intercept db.* and send_message for the bot handlers."""
    sent: list[tuple[int, str]] = []
    state = SimpleNamespace(
        budget=None,
        rank_source="munskankarna",
        value_filter=None,
        wine_type_filter=None,
        upcoming=[],
        past=[],
    )

    monkeypatch.setattr(bot, "send_message", lambda chat_id, msg: sent.append((chat_id, msg)))
    monkeypatch.setattr(bot.db, "get_subscriber_budget", lambda c, chat: state.budget)
    monkeypatch.setattr(bot.db, "get_subscriber_rank_source", lambda c, chat: state.rank_source)
    monkeypatch.setattr(bot.db, "get_subscriber_value_filter", lambda c, chat: state.value_filter)
    monkeypatch.setattr(
        bot.db, "get_subscriber_wine_type_filter", lambda c, chat: state.wine_type_filter
    )
    monkeypatch.setattr(bot.db, "get_upcoming_release_dates", lambda c, since: state.upcoming)
    monkeypatch.setattr(bot.db, "get_past_release_dates_with_data", lambda c, since: state.past)
    monkeypatch.setattr(bot.db, "get_subscriber_preview_date", lambda c, chat: None)
    monkeypatch.setattr(bot.db, "has_wines_for", lambda c, d: False)
    monkeypatch.setattr(bot.db, "get_last_release_with_data", lambda c: None)

    def _set_budget(conn, chat, value):
        state.budget = value

    monkeypatch.setattr(bot.db, "set_subscriber_budget", _set_budget)

    def _set_value_filter(conn, chat, value):
        state.value_filter = value

    monkeypatch.setattr(bot.db, "set_subscriber_value_filter", _set_value_filter)

    def _set_wine_type_filter(conn, chat, value):
        state.wine_type_filter = value

    monkeypatch.setattr(bot.db, "set_subscriber_wine_type_filter", _set_wine_type_filter)

    return state, sent


# ---- /budget ------------------------------------------------------------


def test_budget_no_arg_shows_current_does_not_clear(fake_state):
    state, sent = fake_state
    state.budget = 200.0

    bot._handle_budget(123, "/budget", MagicMock())

    assert state.budget == 200.0  # not cleared
    assert any("Aktuell budget: 200 kr" in msg for _, msg in sent)
    assert any("/clear" in msg for _, msg in sent)


def test_budget_no_arg_when_unset_says_so(fake_state):
    state, sent = fake_state
    state.budget = None

    bot._handle_budget(123, "/budget", MagicMock())

    assert any("Ingen budget satt" in msg for _, msg in sent)


def test_budget_clear_word_is_invalid_amount(fake_state):
    """`/budget clear` no longer clears — that's `/clear` now."""
    state, sent = fake_state
    state.budget = 200.0

    bot._handle_budget(123, "/budget clear", MagicMock())

    assert state.budget == 200.0  # unchanged
    assert any("giltigt belopp" in msg for _, msg in sent)


def test_budget_set_value(fake_state):
    state, sent = fake_state
    bot._handle_budget(123, "/budget 150", MagicMock())
    assert state.budget == 150.0
    assert any("Budget satt till 150 kr" in msg for _, msg in sent)


def test_budget_invalid_value(fake_state):
    state, sent = fake_state
    state.budget = 200.0
    bot._handle_budget(123, "/budget abc", MagicMock())
    assert state.budget == 200.0  # unchanged
    assert any("giltigt belopp" in msg for _, msg in sent)


# ---- /clear -------------------------------------------------------------


def test_clear_resets_budget_and_category(fake_state, monkeypatch):
    state, sent = fake_state
    state.budget = 200.0
    state.value_filter = ["fynd"]
    monkeypatch.setattr(bot, "_send_ranked", lambda *a, **kw: True)

    bot._handle_clear(123, MagicMock())

    assert state.budget is None
    assert state.value_filter is None
    msg = sent[0][1]
    assert "Filter rensade" in msg
    assert "budget" in msg and "kategori" in msg
    assert "Källa kvar" in msg and "Munskänkarna" in msg


def test_clear_preserves_source(fake_state, monkeypatch):
    state, sent = fake_state
    state.budget = 200.0
    state.rank_source = "vivino"
    monkeypatch.setattr(bot, "_send_ranked", lambda *a, **kw: True)

    bot._handle_clear(123, MagicMock())

    assert state.rank_source == "vivino"  # source untouched
    assert any("Vivino" in msg for _, msg in sent)


def test_clear_noop_when_no_filters(fake_state):
    state, sent = fake_state
    # defaults: budget=None, value_filter=None

    bot._handle_clear(123, MagicMock())

    assert any("Inga filter att rensa" in msg for _, msg in sent)


def test_clear_command_is_dispatched():
    assert "/clear" in bot._HANDLERS


# ---- /settings ----------------------------------------------------------


def test_settings_reports_all_fields(fake_state):
    state, sent = fake_state
    state.budget = 200
    state.rank_source = "munskankarna"
    state.value_filter = ["fynd", "prisvärt"]
    state.upcoming = [date(2026, 5, 8)]

    bot._handle_settings(456, MagicMock())

    msg = sent[-1][1]
    assert "Källa:" in msg and "Munskänkarna" in msg
    assert "Budget:" in msg and "200 kr" in msg
    assert "Kategori:" in msg and "fynd, prisvärt" in msg
    assert "Nästa släpp:" in msg and "8 maj 2026" in msg


def test_settings_handles_unset_state(fake_state):
    state, sent = fake_state
    # all defaults: budget=None, rank_source="munskankarna", value_filter=None, no upcoming

    bot._handle_settings(456, MagicMock())

    msg = sent[-1][1]
    assert "Munskänkarna" in msg
    assert "Budget:" in msg and "ingen" in msg
    assert "Kategori:" in msg and "alla" in msg
    assert "Nästa släpp:" in msg and "okänt" in msg


# ---- _empty_view_message -----------------------------------------------


def test_empty_view_message_mentions_filters_and_settings():
    msg = bot._empty_view_message(date(2026, 4, 24))
    assert "matchar dina filter" in msg
    assert "/settings" in msg
    assert "24 april 2026" in msg


# ---- /recent ------------------------------------------------------------


def test_recent_no_past_release(fake_state, monkeypatch):
    _, sent = fake_state
    monkeypatch.setattr(bot.db, "get_last_release_with_data", lambda c: None)

    bot._handle_recent(789, MagicMock())

    assert any("Inga tidigare släpp" in msg for _, msg in sent)


def test_recent_sends_ranked_for_last_release(fake_state, monkeypatch):
    _, sent = fake_state
    last = date(2026, 4, 24)
    monkeypatch.setattr(bot.db, "get_last_release_with_data", lambda c: last)

    called = {}

    def fake_send_ranked(chat_id, conn, release_date, source):
        called["args"] = (chat_id, release_date, source)
        return True

    monkeypatch.setattr(bot, "_send_ranked", fake_send_ranked)

    bot._handle_recent(789, MagicMock())

    assert called["args"] == (789, last, "munskankarna")
    # _send_ranked succeeded → no fallback empty-view message
    assert not any("matchar dina filter" in msg for _, msg in sent)


def test_recent_empty_view_when_filters_exclude_all(fake_state, monkeypatch):
    _, sent = fake_state
    last = date(2026, 4, 24)
    monkeypatch.setattr(bot.db, "get_last_release_with_data", lambda c: last)
    monkeypatch.setattr(bot, "_send_ranked", lambda *a, **kw: False)

    bot._handle_recent(789, MagicMock())

    assert any("matchar dina filter" in msg for _, msg in sent)
    assert any("24 april 2026" in msg for _, msg in sent)


# ---- /winetype ----------------------------------------------------------


def test_winetype_parse_aliases():
    from klunkar.bot import parse_wine_type_args

    resolved, unknown = parse_wine_type_args("rött, white, mousserande")
    assert resolved == ["Rött vin", "Vitt vin", "Mousserande vin"]
    assert unknown == []


def test_winetype_parse_unknown_collected():
    from klunkar.bot import parse_wine_type_args

    resolved, unknown = parse_wine_type_args("rött, blå, vitt")
    assert resolved == ["Rött vin", "Vitt vin"]
    assert unknown == ["blå"]


def test_winetype_no_arg_shows_current(fake_state):
    state, sent = fake_state
    state.wine_type_filter = ["Rött vin"]

    bot._handle_winetype(123, "/winetype", MagicMock())

    msg = sent[-1][1]
    assert "Vintyper" in msg
    assert "Rött vin" in msg


def test_winetype_set(fake_state):
    state, sent = fake_state

    bot._handle_winetype(123, "/winetype rött,vitt", MagicMock())

    assert state.wine_type_filter == ["Rött vin", "Vitt vin"]
    assert any("Vintypfilter satt till" in m for _, m in sent)


def test_winetype_unknown_rejected(fake_state):
    state, sent = fake_state

    bot._handle_winetype(123, "/winetype blå", MagicMock())

    assert state.wine_type_filter is None  # unchanged
    assert any("Okänd vintyp" in m for _, m in sent)


def test_clear_resets_wine_type_filter(fake_state, monkeypatch):
    state, sent = fake_state
    state.wine_type_filter = ["Rött vin"]
    monkeypatch.setattr(bot, "_send_ranked", lambda *a, **kw: True)

    bot._handle_clear(123, MagicMock())

    assert state.wine_type_filter is None
    assert any("vintyp" in m for _, m in sent)


def test_settings_shows_wine_type(fake_state):
    state, sent = fake_state
    state.wine_type_filter = ["Rött vin", "Vitt vin"]

    bot._handle_settings(456, MagicMock())

    msg = sent[-1][1]
    assert "Vintyp:" in msg and "Rött vin, Vitt vin" in msg


# ---- /old ---------------------------------------------------------------


def test_old_past_date_with_no_wines_says_no_release(fake_state, monkeypatch):
    _, sent = fake_state
    monkeypatch.setattr(bot.db, "has_wines_for", lambda c, d: False)

    bot._handle_old(123, "/old 2020-01-15", MagicMock())

    msg = sent[-1][1]
    assert "Inget släpp" in msg
    assert "15 januari 2020" in msg
    assert "/releases" in msg
    assert "ännu" not in msg  # different wording from upcoming-but-not-yet-fetched


def test_old_future_date_with_no_wines_says_not_yet(fake_state, monkeypatch):
    _, sent = fake_state
    monkeypatch.setattr(bot.db, "has_wines_for", lambda c, d: False)
    future = (date.today().replace(year=date.today().year + 1)).isoformat()

    bot._handle_old(123, f"/old {future}", MagicMock())

    msg = sent[-1][1]
    assert "ännu" in msg
    assert "Försök igen senare" in msg


def test_old_invalid_date(fake_state):
    _, sent = fake_state
    bot._handle_old(123, "/old not-a-date", MagicMock())
    assert any("Ogiltigt datum" in m for _, m in sent)


def test_old_missing_date(fake_state):
    _, sent = fake_state
    bot._handle_old(123, "/old", MagicMock())
    assert any("Ange ett datum" in m for _, m in sent)


# ---- /releases ----------------------------------------------------------


def test_releases_shows_past_and_upcoming(fake_state):
    state, sent = fake_state
    state.past = [date(2026, 4, 10), date(2026, 4, 17), date(2026, 4, 24)]
    state.upcoming = [date(2026, 5, 8)]

    bot._handle_releases(789, MagicMock())

    msg = sent[-1][1]
    assert "Kommande släpp" in msg and "8 maj 2026" in msg
    assert "Tidigare släpp" in msg
    # past listed in reverse chronological (most recent first)
    i_24 = msg.index("24 april 2026")
    i_17 = msg.index("17 april 2026")
    i_10 = msg.index("10 april 2026")
    assert i_24 < i_17 < i_10


def test_releases_empty_when_neither(fake_state):
    state, sent = fake_state
    state.past = []
    state.upcoming = []

    bot._handle_releases(789, MagicMock())

    assert any("Inga släpp" in m for _, m in sent)


# ---- handler dispatch ---------------------------------------------------


def test_settings_command_is_dispatched():
    assert "/settings" in bot._HANDLERS


def test_recent_command_is_dispatched():
    assert "/recent" in bot._HANDLERS
