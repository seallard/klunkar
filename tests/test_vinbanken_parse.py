from datetime import date
from pathlib import Path

from klunkar.sources.vinbanken import _discover_article_urls, _parse_article

FIXTURES = Path(__file__).parent / "fixtures"
HUB = FIXTURES / "vinbanken_hub_2026-05-12.html"
ARTICLE_RED = FIXTURES / "vinbanken_article_roda_2026-05-15.html"
ARTICLE_WHITE = FIXTURES / "vinbanken_article_vitt_2026-05-15.html"

RED_URL = "https://vinbanken.se/artiklar/roda-viner-i-tillfalligt-sortiment-15-maj-6771"
WHITE_URL = "https://vinbanken.se/artiklar/vitt-rose-och-bubbel-i-tillfalligt-sortiment-15-maj-6772"


def test_discover_filters_by_date():
    html = HUB.read_text(encoding="utf-8")
    urls = _discover_article_urls(html, date(2026, 5, 15))
    assert RED_URL in urls
    assert WHITE_URL in urls
    # Other dates' articles are excluded
    assert not any("8-maj" in u for u in urls)
    assert not any("24-april" in u for u in urls)


def test_discover_returns_empty_for_unknown_date():
    html = HUB.read_text(encoding="utf-8")
    urls = _discover_article_urls(html, date(2026, 6, 1))
    assert urls == []


def test_discover_finds_both_articles_for_recurring_dates():
    html = HUB.read_text(encoding="utf-8")
    urls = _discover_article_urls(html, date(2026, 4, 10))
    # 10 april has roda + vita-och-mousserande
    assert len(urls) == 2
    assert any("roda-viner" in u for u in urls)
    assert any("vita-och-mousserande" in u for u in urls)


def test_parse_red_article_extracts_known_wine():
    html = ARTICLE_RED.read_text(encoding="utf-8")
    parsed = _parse_article(html, RED_URL)
    assert "90378" in parsed  # Las Moras Gran Syrah
    p = parsed["90378"]
    assert p.score == 90
    assert p.fynd is True
    assert p.tasting_note and "violer" in p.tasting_note
    # Per-wine anchor: text fragment matches the SB-number marker on the page
    assert p.review_url == f"{RED_URL}#:~:text=%2390378"


def test_parse_red_article_full_release():
    html = ARTICLE_RED.read_text(encoding="utf-8")
    parsed = _parse_article(html, RED_URL)
    assert len(parsed) == 10
    assert all(0 <= p.score <= 100 for p in parsed.values())
    # Exactly one fynd in red
    assert sum(1 for p in parsed.values() if p.fynd) == 1


def test_parse_white_article_full_release():
    html = ARTICLE_WHITE.read_text(encoding="utf-8")
    parsed = _parse_article(html, WHITE_URL)
    assert len(parsed) == 12
    assert sum(1 for p in parsed.values() if p.fynd) == 2
    # All SB numbers are 4-7 digit strings
    assert all(k.isdigit() and 4 <= len(k) <= 7 for k in parsed)


def test_parse_skips_non_wine_blocks():
    # No non-wine `<h3>` headers leak in as bogus entries (Slutsats etc.)
    html = ARTICLE_RED.read_text(encoding="utf-8")
    parsed = _parse_article(html, RED_URL)
    # All entries must have a positive score and a non-empty tasting note
    assert all(p.score > 0 for p in parsed.values())
    assert all(p.tasting_note for p in parsed.values())
