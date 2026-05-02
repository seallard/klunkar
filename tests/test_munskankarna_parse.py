from datetime import date
from pathlib import Path

from klunkar.sources.munskankarna import _parse, _release_slug, release_url

FIXTURE = Path(__file__).parent / "fixtures" / "munskankarna_2026-04-24.html"


def test_release_slug():
    assert _release_slug(date(2026, 4, 24)) == "24-april-2026"
    assert _release_slug(date(2025, 12, 5)) == "5-december-2025"


def test_release_url():
    assert (
        release_url(date(2026, 4, 24))
        == "https://www.munskankarna.se/sv/vinlocus/tillfalligt-sortiment-24-april-2026"
    )


def test_parse_known_wine():
    html = FIXTURE.read_text(encoding="utf-8")
    parsed = _parse(
        html, "https://www.munskankarna.se/sv/vinlocus/tillfalligt-sortiment-24-april-2026"
    )

    assert "94806" in parsed
    p = parsed["94806"]
    assert p.score == 17.0
    assert p.value_rating == "ej prisvärt"
    assert p.tasting_note and "Fint fruktig doft" in p.tasting_note
    assert p.review_url and p.review_url.startswith(
        "https://www.munskankarna.se/sv/vinlocus/tillfalligt-sortiment-24-april-2026/"
    )


def test_parse_extracts_full_release():
    html = FIXTURE.read_text(encoding="utf-8")
    parsed = _parse(
        html, "https://www.munskankarna.se/sv/vinlocus/tillfalligt-sortiment-24-april-2026"
    )

    assert len(parsed) >= 30
    assert all(0 <= p.score <= 20 for p in parsed.values())
    valid_ratings = {None, "fynd", "mer än prisvärt", "prisvärt", "ej prisvärt"}
    assert {p.value_rating for p in parsed.values()} <= valid_ratings
    # Half-point increments must round-trip
    assert any(p.score % 1 == 0.5 for p in parsed.values())
