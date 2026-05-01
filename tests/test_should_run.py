from datetime import date, datetime, timedelta, timezone
from unittest.mock import MagicMock

from klunkar import config, release


def _now():
    return datetime.now(timezone.utc)


def test_no_prior_run_returns_true(monkeypatch):
    monkeypatch.setattr(release.db, "get_last_run", lambda c, d, s: None)
    assert release._should_run(MagicMock(), date.today() + timedelta(1), "vivino") is True


def test_zero_match_within_refresh_interval_skips(monkeypatch):
    """Retry-on-empty respects the refresh interval — doesn't hammer per cron tick."""
    recent = _now() - timedelta(hours=1)
    monkeypatch.setattr(release.db, "get_last_run", lambda c, d, s: (recent, 0))
    assert release._should_run(MagicMock(), date.today() + timedelta(1), "munskankarna") is False


def test_zero_match_after_refresh_interval_retries(monkeypatch):
    old = _now() - timedelta(hours=config.ENRICHMENT_REFRESH_HOURS + 1)
    monkeypatch.setattr(release.db, "get_last_run", lambda c, d, s: (old, 0))
    # Past date with 0-match still retries when interval elapsed (backfill case).
    assert release._should_run(MagicMock(), date.today() - timedelta(2), "munskankarna") is True


def test_past_release_with_matches_is_frozen(monkeypatch):
    old = _now() - timedelta(days=5)
    monkeypatch.setattr(release.db, "get_last_run", lambda c, d, s: (old, 30))
    assert release._should_run(MagicMock(), date.today() - timedelta(3), "vivino") is False


def test_future_release_with_matches_respects_refresh(monkeypatch):
    recent = _now() - timedelta(hours=1)
    monkeypatch.setattr(release.db, "get_last_run", lambda c, d, s: (recent, 30))
    assert release._should_run(MagicMock(), date.today() + timedelta(2), "vivino") is False

    old = _now() - timedelta(hours=config.ENRICHMENT_REFRESH_HOURS + 1)
    monkeypatch.setattr(release.db, "get_last_run", lambda c, d, s: (old, 30))
    assert release._should_run(MagicMock(), date.today() + timedelta(2), "vivino") is True
