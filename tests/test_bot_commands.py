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
        rank_source="vivino",
        value_filter=None,
        upcoming=[],
    )

    monkeypatch.setattr(bot, "send_message",
                        lambda chat_id, msg: sent.append((chat_id, msg)))
    monkeypatch.setattr(bot.db, "get_subscriber_budget",
                        lambda c, chat: state.budget)
    monkeypatch.setattr(bot.db, "get_subscriber_rank_source",
                        lambda c, chat: state.rank_source)
    monkeypatch.setattr(bot.db, "get_subscriber_value_filter",
                        lambda c, chat: state.value_filter)
    monkeypatch.setattr(bot.db, "get_upcoming_release_dates",
                        lambda c, since: state.upcoming)
    monkeypatch.setattr(bot.db, "get_subscriber_preview_date", lambda c, chat: None)
    monkeypatch.setattr(bot.db, "has_wines_for", lambda c, d: False)
    monkeypatch.setattr(bot.db, "get_last_release_with_data", lambda c: None)

    def _set_budget(conn, chat, value):
        state.budget = value
    monkeypatch.setattr(bot.db, "set_subscriber_budget", _set_budget)

    return state, sent


# ---- /budget ------------------------------------------------------------

def test_budget_no_arg_shows_current_does_not_clear(fake_state):
    state, sent = fake_state
    state.budget = 200.0

    bot._handle_budget(123, "/budget", MagicMock())

    assert state.budget == 200.0   # not cleared
    assert any("Aktuell budget: 200 kr" in msg for _, msg in sent)
    assert any("/budget clear" in msg for _, msg in sent)


def test_budget_no_arg_when_unset_says_so(fake_state):
    state, sent = fake_state
    state.budget = None

    bot._handle_budget(123, "/budget", MagicMock())

    assert any("Ingen budget satt" in msg for _, msg in sent)


def test_budget_clear_token_removes_filter(fake_state):
    state, sent = fake_state
    state.budget = 200.0

    bot._handle_budget(123, "/budget clear", MagicMock())

    assert state.budget is None
    assert any("Budget borttagen" in msg for _, msg in sent)


@pytest.mark.parametrize("token", ["clear", "off", "none", "-", "rensa"])
def test_budget_clear_aliases(fake_state, token):
    state, _ = fake_state
    state.budget = 200.0
    bot._handle_budget(123, f"/budget {token}", MagicMock())
    assert state.budget is None


def test_budget_set_value(fake_state):
    state, sent = fake_state
    bot._handle_budget(123, "/budget 150", MagicMock())
    assert state.budget == 150.0
    assert any("Budget satt till 150 kr" in msg for _, msg in sent)


def test_budget_invalid_value(fake_state):
    state, sent = fake_state
    state.budget = 200.0
    bot._handle_budget(123, "/budget abc", MagicMock())
    assert state.budget == 200.0   # unchanged
    assert any("giltigt belopp" in msg for _, msg in sent)


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
    # all defaults: budget=None, rank_source="vivino", value_filter=None, no upcoming

    bot._handle_settings(456, MagicMock())

    msg = sent[-1][1]
    assert "Vivino" in msg
    assert "Budget:" in msg and "ingen" in msg
    assert "Kategori:" in msg and "alla" in msg
    assert "Nästa släpp:" in msg and "okänt" in msg


# ---- handler dispatch ---------------------------------------------------

def test_settings_command_is_dispatched():
    assert "/settings" in bot._HANDLERS
