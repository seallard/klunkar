"""Tests the retroactive notification path in check_and_notify.

Mocks all DB and Telegram interactions so we can exercise the orchestration
logic without spinning up Postgres.
"""
from datetime import date, datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from klunkar import release


@pytest.fixture
def fake_db(monkeypatch):
    """Intercept every db.* call used by check_and_notify."""
    calls = SimpleNamespace(
        marked_notified=[],
        marked_seen=[],
    )

    state = {
        "subscribers": [],                       # tomorrow's eligible
        "past_dates": [],                        # returned by get_past_release_dates_with_data
        "past_eligible": {},                     # date -> list of subscriber rows
        "wines": {},                             # date -> ranked list
        "is_upcoming_tomorrow": True,
        "already_notified": set(),               # set of (date, chat_id)
        "release_seen": set(),                   # set of dates
        "wine_count": {},                        # date -> int
    }

    def has_notified_subscriber(conn, d, chat_id):
        return (d, chat_id) in state["already_notified"]

    def mark_notified_subscriber(conn, d, chat_id):
        state["already_notified"].add((d, chat_id))
        calls.marked_notified.append((d, chat_id))

    def is_release_seen(conn, d):
        return d in state["release_seen"]

    def mark_release_seen(conn, d, n):
        state["release_seen"].add(d)
        calls.marked_seen.append((d, n))

    monkeypatch.setattr(release.db, "is_upcoming_release_date",
                        lambda c, d: state["is_upcoming_tomorrow"])
    monkeypatch.setattr(release.db, "get_subscribers", lambda c: state["subscribers"])
    monkeypatch.setattr(release.db, "get_subscribers_to_notify_for",
                        lambda c, d: state["past_eligible"].get(d, []))
    monkeypatch.setattr(release.db, "get_past_release_dates_with_data",
                        lambda c, since: state["past_dates"])
    monkeypatch.setattr(release.db, "has_notified_subscriber", has_notified_subscriber)
    monkeypatch.setattr(release.db, "mark_notified_subscriber", mark_notified_subscriber)
    monkeypatch.setattr(release.db, "is_release_seen", is_release_seen)
    monkeypatch.setattr(release.db, "mark_release_seen", mark_release_seen)
    monkeypatch.setattr(release.db, "get_wines",
                        lambda c, d: [None] * state["wine_count"].get(d, 0))

    sent_messages = []
    monkeypatch.setattr(release, "send_message",
                        lambda chat_id, msg: sent_messages.append((chat_id, msg)))
    calls.sent = sent_messages

    # Pretend ranking always returns one wine for any date the test marks "has data".
    def fake_build(conn, d, *, source, value_ratings=None):
        if state["wines"].get(d):
            return state["wines"][d]
        return []
    monkeypatch.setattr(release.ranking, "build_ranked_view", fake_build)

    monkeypatch.setattr(release, "format_message",
                        lambda wines, d, **kw: f"msg for {d}")

    return state, calls


def test_tomorrow_only_when_no_past_releases(fake_db):
    state, calls = fake_db
    state["subscribers"] = [(1, None, "vivino", None)]
    state["wines"][date.today() + timedelta(1)] = ["a wine"]
    state["wine_count"][date.today() + timedelta(1)] = 5

    assert release.check_and_notify(MagicMock()) is True
    assert calls.sent == [(1, f"msg for {date.today() + timedelta(1)}")]
    assert (date.today() + timedelta(1), 5) in calls.marked_seen


def test_retro_notifies_eligible_subscriber_for_past_release(fake_db):
    state, _ = fake_db
    state["is_upcoming_tomorrow"] = False
    past = date.today() - timedelta(3)
    state["past_dates"] = [past]
    state["past_eligible"][past] = [(42, None, "munskankarna", None)]
    state["wines"][past] = ["wine"]
    state["wine_count"][past] = 30

    assert release.check_and_notify(MagicMock()) is True
    assert (past, 42) in state["already_notified"]


def test_retro_skips_when_chosen_source_still_empty(fake_db):
    state, calls = fake_db
    state["is_upcoming_tomorrow"] = False
    past = date.today() - timedelta(2)
    state["past_dates"] = [past]
    state["past_eligible"][past] = [(7, None, "munskankarna", None)]
    # state["wines"][past] is empty → ranking returns []

    assert release.check_and_notify(MagicMock()) is False
    assert calls.sent == []
    assert (past, 7) not in state["already_notified"]


def test_retro_does_not_resend_for_already_notified(fake_db):
    """get_subscribers_to_notify_for already filters — but the helper double-checks."""
    state, _ = fake_db
    state["is_upcoming_tomorrow"] = False
    past = date.today() - timedelta(1)
    state["past_dates"] = [past]
    state["already_notified"].add((past, 99))
    # Even if a subscriber leaks through, has_notified_subscriber blocks it.
    state["past_eligible"][past] = [(99, None, "vivino", None)]
    state["wines"][past] = ["wine"]

    assert release.check_and_notify(MagicMock()) is False


def test_tomorrow_and_retro_both_fire(fake_db):
    state, calls = fake_db
    tomorrow = date.today() + timedelta(1)
    past = date.today() - timedelta(2)

    state["subscribers"] = [(1, None, "vivino", None)]
    state["wines"][tomorrow] = ["wine"]
    state["wine_count"][tomorrow] = 5

    state["past_dates"] = [past]
    state["past_eligible"][past] = [(42, None, "munskankarna", None)]
    state["wines"][past] = ["wine"]
    state["wine_count"][past] = 30

    assert release.check_and_notify(MagicMock()) is True
    sent_dates = {d for _, d in [(c, m.split()[-1]) for c, m in calls.sent]}
    assert str(tomorrow) in sent_dates
    assert str(past) in sent_dates
