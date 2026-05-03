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
        country_filter=None,
        preview_date=None,
        upcoming=[],
        past=[],
        countries=[],
    )

    edits: list[tuple[int, int, str]] = []

    def _send(chat_id, msg, reply_markup=None):
        sent.append((chat_id, msg))

    def _edit(chat_id, message_id, msg, reply_markup=None):
        edits.append((chat_id, message_id, msg))

    monkeypatch.setattr(bot, "send_message", _send)
    monkeypatch.setattr(bot, "edit_message_text", _edit)
    monkeypatch.setattr(bot, "answer_callback_query", lambda qid, text=None: None)
    state.edits = edits
    monkeypatch.setattr(bot.db, "get_subscriber_budget", lambda c, chat: state.budget)
    monkeypatch.setattr(bot.db, "get_subscriber_rank_source", lambda c, chat: state.rank_source)
    monkeypatch.setattr(bot.db, "get_subscriber_value_filter", lambda c, chat: state.value_filter)
    monkeypatch.setattr(
        bot.db, "get_subscriber_wine_type_filter", lambda c, chat: state.wine_type_filter
    )
    monkeypatch.setattr(
        bot.db, "get_subscriber_country_filter", lambda c, chat: state.country_filter
    )
    monkeypatch.setattr(bot.db, "get_release_countries", lambda c, d: state.countries)
    monkeypatch.setattr(bot, "_resolve_release_countries", lambda c: state.countries)
    monkeypatch.setattr(bot.db, "get_upcoming_release_dates", lambda c, since: state.upcoming)
    monkeypatch.setattr(bot.db, "get_past_release_dates_with_data", lambda c, since: state.past)
    monkeypatch.setattr(bot.db, "get_release_type_counts", lambda c, d: {})
    monkeypatch.setattr(bot.db, "get_subscriber_preview_date", lambda c, chat: state.preview_date)
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

    def _set_rank_source(conn, chat, value):
        state.rank_source = str(value)

    monkeypatch.setattr(bot.db, "set_subscriber_rank_source", _set_rank_source)

    def _set_country_filter(conn, chat, value):
        state.country_filter = value

    monkeypatch.setattr(bot.db, "set_subscriber_country_filter", _set_country_filter)

    def _set_preview_date(conn, chat, value):
        state.preview_date = value

    monkeypatch.setattr(bot.db, "set_subscriber_preview_date", _set_preview_date)

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


def test_clear_resets_budget_and_value(fake_state, monkeypatch):
    state, sent = fake_state
    state.budget = 200.0
    state.value_filter = ["fynd"]
    monkeypatch.setattr(bot, "_send_ranked", lambda *a, **kw: True)

    bot._handle_clear(123, MagicMock())

    assert state.budget is None
    assert state.value_filter is None
    msg = sent[0][1]
    assert "Filter rensade" in msg
    assert "budget" in msg and "prisvärdhet" in msg
    assert "Recensent kvar" in msg and "Munskänkarna" in msg


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
    assert "Recensent:" in msg and "Munskänkarna" in msg
    assert "Budget:" in msg and "200 kr" in msg
    assert "Prisvärdhet:" in msg and "fynd, prisvärt" in msg
    assert "Släpp:" in msg and "8 maj 2026" in msg


def test_settings_handles_unset_state(fake_state):
    state, sent = fake_state
    # all defaults: budget=None, rank_source="munskankarna", value_filter=None, no upcoming

    bot._handle_settings(456, MagicMock())

    msg = sent[-1][1]
    assert "Munskänkarna" in msg
    assert "Budget:" in msg and "ingen" in msg
    assert "Prisvärdhet:" in msg and "alla" in msg
    assert "Släpp:" in msg and "okänt" in msg


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


# ---- inline callbacks ---------------------------------------------------


def test_source_no_arg_sends_keyboard(fake_state, monkeypatch):
    state, sent = fake_state
    captured: list[dict] = []

    def _send(chat_id, msg, reply_markup=None):
        sent.append((chat_id, msg))
        captured.append(reply_markup or {})

    monkeypatch.setattr(bot, "send_message", _send)

    bot._handle_source(123, "/source", MagicMock())

    assert any("Välj recensent" in m for _, m in sent)
    keyboard = captured[-1].get("inline_keyboard", [])
    flat = [btn["callback_data"] for row in keyboard for btn in row]
    assert "src:vivino" in flat and "src:munskankarna" in flat


def test_source_callback_writes_db_and_edits(fake_state, monkeypatch):
    state, sent = fake_state
    state.rank_source = "munskankarna"
    monkeypatch.setattr(bot, "_send_ranked", lambda *a, **kw: True)
    monkeypatch.setattr(bot, "_resolve_active_date", lambda c: date(2026, 5, 8))

    bot._handle_source_callback(123, 42, "vivino", MagicMock())

    assert state.rank_source == "vivino"
    assert state.edits and state.edits[-1][1] == 42
    assert "Vivino" in state.edits[-1][2]


def test_value_callback_toggle_adds(fake_state):
    state, sent = fake_state
    state.value_filter = None

    bot._handle_value_callback(123, 99, "fynd", MagicMock())

    assert state.value_filter == ["fynd"]
    assert state.edits[-1][1] == 99
    assert "fynd" in state.edits[-1][2]


def test_value_callback_toggle_removes_existing(fake_state):
    state, sent = fake_state
    state.value_filter = ["fynd", "prisvärt"]

    bot._handle_value_callback(123, 99, "fynd", MagicMock())

    assert state.value_filter == ["prisvärt"]


def test_value_callback_done_sends_list(fake_state, monkeypatch):
    state, sent = fake_state
    state.value_filter = ["fynd"]
    called = {}

    def fake_send_ranked(chat_id, conn, release_date, source):
        called["args"] = (chat_id, release_date, source)
        return True

    monkeypatch.setattr(bot, "_send_ranked", fake_send_ranked)
    monkeypatch.setattr(bot, "_resolve_active_date", lambda c: date(2026, 5, 8))

    bot._handle_value_callback(123, 99, "done", MagicMock())

    assert called["args"][0] == 123
    # picker message edited away
    assert state.edits[-1][1] == 99
    assert "fynd" in state.edits[-1][2]


def test_winetype_callback_toggle_adds_value(fake_state):
    state, sent = fake_state
    state.wine_type_filter = None

    bot._handle_winetype_callback(123, 50, "rod", MagicMock())

    assert state.wine_type_filter == ["Rött vin"]
    assert state.edits[-1][1] == 50
    assert "Rött vin" in state.edits[-1][2]


def test_winetype_callback_toggle_removes_existing(fake_state):
    state, sent = fake_state
    state.wine_type_filter = ["Rött vin", "Vitt vin"]

    bot._handle_winetype_callback(123, 50, "rod", MagicMock())

    assert state.wine_type_filter == ["Vitt vin"]


def test_winetype_callback_done_sends_list(fake_state, monkeypatch):
    state, sent = fake_state
    state.wine_type_filter = ["Rött vin"]
    called = {}

    def fake_send_ranked(chat_id, conn, release_date, source):
        called["args"] = (chat_id, release_date, source)
        return True

    monkeypatch.setattr(bot, "_send_ranked", fake_send_ranked)
    monkeypatch.setattr(bot, "_resolve_active_date", lambda c: date(2026, 5, 8))

    bot._handle_winetype_callback(123, 50, "done", MagicMock())

    assert called["args"][0] == 123
    assert state.edits[-1][1] == 50
    assert "Rött vin" in state.edits[-1][2]


def test_settings_sends_hub_keyboard(fake_state, monkeypatch):
    state, sent = fake_state
    captured: list[dict] = []

    def _send(chat_id, msg, reply_markup=None):
        sent.append((chat_id, msg))
        captured.append(reply_markup or {})

    monkeypatch.setattr(bot, "send_message", _send)

    bot._handle_settings(456, MagicMock())

    flat = [btn["callback_data"] for row in captured[-1].get("inline_keyboard", []) for btn in row]
    assert "hub:src" in flat and "hub:wt" in flat and "hub:val" in flat and "hub:bud" in flat
    assert "hub:rel" in flat and "hub:show" in flat


def test_hub_open_re_renders_hub(fake_state):
    state, sent = fake_state
    bot._handle_hub_callback(123, 99, "open", MagicMock())
    assert state.edits[-1][1] == 99
    assert "Dina inställningar" in state.edits[-1][2]


def test_hub_src_open_then_pick_writes_and_returns(fake_state):
    state, _ = fake_state
    state.rank_source = "munskankarna"

    # Open the source picker from the hub
    bot._handle_hub_callback(123, 99, "src", MagicMock())
    assert "Välj recensent" in state.edits[-1][2]

    # Pick a source
    bot._handle_hub_callback(123, 99, "src:vivino", MagicMock())
    assert state.rank_source == "vivino"
    # back to hub
    assert "Dina inställningar" in state.edits[-1][2]


def test_hub_bud_chip_writes_budget(fake_state):
    state, _ = fake_state
    bot._handle_hub_callback(123, 99, "bud:150", MagicMock())
    assert state.budget == 150.0
    assert "Dina inställningar" in state.edits[-1][2]


def test_hub_bud_none_clears_budget(fake_state):
    state, _ = fake_state
    state.budget = 200.0
    bot._handle_hub_callback(123, 99, "bud:none", MagicMock())
    assert state.budget is None


def test_hub_wt_toggle_then_done(fake_state):
    state, _ = fake_state
    state.wine_type_filter = None

    bot._handle_hub_callback(123, 99, "wt:rod", MagicMock())
    assert state.wine_type_filter == ["Rött vin"]
    # still in picker (not hub)
    assert "Välj vintyp" in state.edits[-1][2]

    bot._handle_hub_callback(123, 99, "open", MagicMock())
    assert "Dina inställningar" in state.edits[-1][2]


def test_hub_cat_toggle_keeps_in_picker(fake_state):
    state, _ = fake_state
    state.value_filter = ["fynd"]

    bot._handle_hub_callback(123, 99, "val:fynd", MagicMock())
    assert state.value_filter is None  # toggled off
    assert "Välj prisvärdhet" in state.edits[-1][2]


def test_hub_show_uses_selected_release(fake_state, monkeypatch):
    state, _ = fake_state
    state.upcoming = [date(2026, 5, 8)]
    called = {}

    def fake_send_for_date(chat_id, target, conn):
        called["target"] = target

    monkeypatch.setattr(bot, "_send_for_date", fake_send_for_date)

    bot._handle_hub_callback(123, 99, "show", MagicMock())

    assert called["target"] == date(2026, 5, 8)


def test_hub_show_falls_back_to_next_upcoming_when_no_preview(fake_state, monkeypatch):
    state, _ = fake_state
    state.preview_date = None
    state.upcoming = [date(2026, 5, 8)]
    called = {}
    monkeypatch.setattr(bot, "_send_for_date", lambda c, t, conn: called.setdefault("target", t))

    bot._handle_hub_callback(123, 99, "show", MagicMock())

    assert called["target"] == date(2026, 5, 8)


def test_hub_show_uses_preview_date_when_set(fake_state, monkeypatch):
    state, _ = fake_state
    state.preview_date = date(2026, 4, 17)
    state.upcoming = [date(2026, 5, 8)]
    called = {}
    monkeypatch.setattr(bot, "_send_for_date", lambda c, t, conn: called.setdefault("target", t))

    bot._handle_hub_callback(123, 99, "show", MagicMock())

    assert called["target"] == date(2026, 4, 17)


def test_hub_show_no_releases_sends_message(fake_state, monkeypatch):
    state, sent = fake_state
    state.upcoming = []
    state.preview_date = None
    monkeypatch.setattr(bot, "_send_for_date", lambda *a, **kw: None)

    bot._handle_hub_callback(123, 99, "show", MagicMock())

    assert any("Inga släpp" in m for _, m in sent)


def test_hub_rel_open_picker(fake_state):
    state, _ = fake_state
    state.upcoming = [date(2026, 5, 8)]
    state.past = [date(2026, 4, 24), date(2026, 4, 17)]

    bot._handle_hub_callback(123, 99, "rel", MagicMock())

    assert "Välj släpp" in state.edits[-1][2]


def test_hub_rel_pick_date_sets_preview_and_returns_to_hub(fake_state):
    state, _ = fake_state
    state.upcoming = [date(2026, 5, 8)]

    bot._handle_hub_callback(123, 99, "rel:2026-04-17", MagicMock())

    assert state.preview_date == date(2026, 4, 17)
    assert "Dina inställningar" in state.edits[-1][2]


def test_hub_rel_no_releases(fake_state):
    state, _ = fake_state
    state.upcoming = []
    state.past = []

    bot._handle_hub_callback(123, 99, "rel", MagicMock())

    assert "Inga släpp" in state.edits[-1][2]


def test_hub_clear_resets_all_filters(fake_state):
    state, _ = fake_state
    state.budget = 200.0
    state.value_filter = ["fynd"]
    state.wine_type_filter = ["Rött vin"]

    bot._handle_hub_callback(123, 99, "clear", MagicMock())

    assert state.budget is None
    assert state.value_filter is None
    assert state.wine_type_filter is None
    assert "Dina inställningar" in state.edits[-1][2]


def test_hub_bud_custom_sends_force_reply(fake_state, monkeypatch):
    state, sent = fake_state
    captured: list[dict] = []

    def _send(chat_id, msg, reply_markup=None):
        sent.append((chat_id, msg))
        captured.append(reply_markup or {})

    monkeypatch.setattr(bot, "send_message", _send)

    bot._handle_hub_callback(123, 99, "bud:custom", MagicMock())

    assert any("Skriv din budget" in m for _, m in sent)
    assert captured[-1].get("force_reply") is True


def test_custom_budget_reply_sets_budget(fake_state):
    state, sent = fake_state

    bot._handle_custom_budget_reply(123, "175", MagicMock())

    assert state.budget == 175.0
    assert any("Budget satt till 175 kr" in m for _, m in sent)


def test_custom_budget_reply_invalid(fake_state):
    state, sent = fake_state
    bot._handle_custom_budget_reply(123, "not-a-number", MagicMock())
    assert state.budget is None
    assert any("Ogiltigt belopp" in m for _, m in sent)


def test_update_routes_force_reply_to_budget(fake_state):
    state, sent = fake_state
    update = {
        "message": {
            "chat": {"id": 123},
            "text": "175",
            "reply_to_message": {"text": f"{bot._BUDGET_PROMPT_PREFIX} (t.ex. 175):"},
        }
    }
    bot._handle_update(update, MagicMock())
    assert state.budget == 175.0


def test_old_no_arg_shows_keyboard(fake_state, monkeypatch):
    state, sent = fake_state
    state.past = [date(2026, 4, 10), date(2026, 4, 17), date(2026, 4, 24)]
    captured: list[dict] = []

    def _send(chat_id, msg, reply_markup=None):
        sent.append((chat_id, msg))
        captured.append(reply_markup or {})

    monkeypatch.setattr(bot, "send_message", _send)

    bot._handle_old(123, "/old", MagicMock())

    flat = [btn["callback_data"] for row in captured[-1].get("inline_keyboard", []) for btn in row]
    assert "old:2026-04-24" in flat
    assert "old:2026-04-10" in flat


def test_old_callback_loads_date(fake_state, monkeypatch):
    state, sent = fake_state
    called = {}

    def fake_send_for_date(chat_id, target, conn):
        called["target"] = target

    monkeypatch.setattr(bot, "_send_for_date", fake_send_for_date)

    bot._handle_old_callback(123, 99, "2026-04-17", MagicMock())

    assert called["target"] == date(2026, 4, 17)
    assert "✓ Visar" in state.edits[-1][2]
    assert "17 april 2026" in state.edits[-1][2]


def test_callback_unknown_prefix_is_no_op(fake_state):
    state, sent = fake_state
    query = {
        "id": "abc",
        "data": "xxx:foo",
        "message": {"chat": {"id": 123}, "message_id": 99},
    }
    bot._handle_callback_query(query, MagicMock())
    # nothing edited, nothing sent
    assert state.edits == []


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
    assert "Välj vintyp" in msg
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


def test_old_missing_date_no_past_releases(fake_state):
    state, sent = fake_state
    state.past = []
    bot._handle_old(123, "/old", MagicMock())
    assert any("Inga tidigare släpp" in m for _, m in sent)


# ---- /releases ----------------------------------------------------------


def test_releases_shows_past_and_upcoming(fake_state, monkeypatch):
    state, sent = fake_state
    state.past = [date(2026, 4, 10), date(2026, 4, 17), date(2026, 4, 24)]
    state.upcoming = [date(2026, 5, 8)]

    captured: list[dict] = []

    def _send(chat_id, msg, reply_markup=None):
        sent.append((chat_id, msg))
        captured.append(reply_markup or {})

    monkeypatch.setattr(bot, "send_message", _send)

    bot._handle_releases(789, MagicMock())

    rows = captured[-1].get("inline_keyboard", [])
    flat_text = [btn["text"] for row in rows for btn in row]
    flat_data = [btn["callback_data"] for row in rows for btn in row]

    # Upcoming gets 📅, past gets 🕒. No section-header buttons.
    assert "📅 8 maj 2026" in flat_text
    assert "🕒 24 april 2026" in flat_text
    assert "— Kommande —" not in flat_text and "— Tidigare —" not in flat_text
    # Upcoming first, then past in reverse-chronological order
    assert flat_data.index("old:2026-05-08") < flat_data.index("old:2026-04-24")
    i_24 = flat_data.index("old:2026-04-24")
    i_17 = flat_data.index("old:2026-04-17")
    i_10 = flat_data.index("old:2026-04-10")
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


# ---- /country ----------------------------------------------------------


def test_country_no_arg_shows_keyboard(fake_state, monkeypatch):
    state, sent = fake_state
    state.countries = ["Frankrike", "Italien", "Spanien"]
    captured: list[dict] = []

    def _send(chat_id, msg, reply_markup=None):
        sent.append((chat_id, msg))
        captured.append(reply_markup or {})

    monkeypatch.setattr(bot, "send_message", _send)
    bot._handle_country(123, "/country", MagicMock())

    flat_data = [
        btn["callback_data"] for row in captured[-1].get("inline_keyboard", []) for btn in row
    ]
    assert "cnt:Italien" in flat_data
    assert "cnt:Frankrike" in flat_data


def test_country_no_arg_when_no_data(fake_state):
    state, sent = fake_state
    state.countries = []
    bot._handle_country(123, "/country", MagicMock())
    assert any("Inga land" in m for _, m in sent)


def test_country_text_path_sets_filter(fake_state, monkeypatch):
    state, sent = fake_state
    state.countries = ["Frankrike", "Italien"]
    monkeypatch.setattr(bot, "_send_ranked", lambda *a, **kw: True)
    monkeypatch.setattr(bot, "_resolve_active_date", lambda c: date(2026, 5, 8))

    bot._handle_country(123, "/country italien,frankrike", MagicMock())

    assert state.country_filter == ["Italien", "Frankrike"]


def test_country_text_path_unknown_rejected(fake_state):
    state, sent = fake_state
    state.countries = ["Italien"]
    bot._handle_country(123, "/country mars", MagicMock())
    assert state.country_filter is None
    assert any("Okänt land" in m for _, m in sent)


def test_country_callback_toggle(fake_state):
    state, _ = fake_state
    state.countries = ["Italien", "Frankrike"]

    bot._handle_country_callback(123, 50, "Italien", MagicMock())

    assert state.country_filter == ["Italien"]
    assert state.edits[-1][1] == 50

    bot._handle_country_callback(123, 50, "Italien", MagicMock())
    assert state.country_filter is None


def test_country_callback_done_returns_to_list(fake_state, monkeypatch):
    state, _ = fake_state
    state.country_filter = ["Italien"]
    called = {}

    def fake_send_ranked(chat_id, conn, release_date, source):
        called["args"] = (chat_id, release_date, source)
        return True

    monkeypatch.setattr(bot, "_send_ranked", fake_send_ranked)
    monkeypatch.setattr(bot, "_resolve_active_date", lambda c: date(2026, 5, 8))

    bot._handle_country_callback(123, 50, "done", MagicMock())

    assert called["args"][0] == 123
    assert "Italien" in state.edits[-1][2]


def test_hub_cnt_open_then_toggle(fake_state):
    state, _ = fake_state
    state.countries = ["Italien", "Frankrike"]

    bot._handle_hub_callback(123, 99, "cnt", MagicMock())
    assert "Välj land" in state.edits[-1][2]

    bot._handle_hub_callback(123, 99, "cnt:Italien", MagicMock())
    assert state.country_filter == ["Italien"]


def test_hub_clear_resets_country_filter(fake_state):
    state, _ = fake_state
    state.country_filter = ["Italien"]
    bot._handle_hub_callback(123, 99, "clear", MagicMock())
    assert state.country_filter is None
