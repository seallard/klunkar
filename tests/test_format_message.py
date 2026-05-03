from datetime import date

from klunkar.models import MunskankarnaPayload, RankedWine, VivinoPayload, Wine
from klunkar.release import format_message

RD = date(2026, 4, 24)


def _wine(num="1", name="Foo", price=200.0):
    return Wine(
        sb_product_number=num,
        sb_product_id=num,
        release_date=RD,
        name=name,
        producer="P",
        sb_url=f"https://sb.se/{num}",
        price=price,
        wine_type="Rött vin",
    )


def test_title_row_is_just_glass_and_name():
    rw = RankedWine(
        wine=_wine(name="Foo", price=149),
        rank_score=4.0,
        vivino=VivinoPayload(
            wine_id=1,
            matched_name="m",
            ratings_average=4.3,
            ratings_count=100,
            wine_url="https://vivino/1",
        ),
    )
    out = format_message([rw], RD, source="vivino")
    assert "🍷 Foo" in out
    assert "🍷 Foo —" not in out  # no price/dash on title row
    assert "🥇" not in out and "🥈" not in out and "🥉" not in out


def test_systembolaget_renders_as_its_own_labeled_row():
    rw = RankedWine(
        wine=_wine(num="1", name="Foo", price=149),
        rank_score=4.0,
    )
    out = format_message([rw], RD, source="vivino")
    assert "[Systembolaget: 149 kr](https://sb.se/1)" in out


def test_systembolaget_row_when_price_missing():
    rw = RankedWine(
        wine=_wine(num="1", name="Foo", price=None),
        rank_score=4.0,
    )
    out = format_message([rw], RD, source="vivino")
    assert "[Systembolaget: köp](https://sb.se/1)" in out


def test_each_present_source_renders_its_own_row_with_link():
    rw = RankedWine(
        wine=_wine(),
        rank_score=4.0,
        vivino=VivinoPayload(
            wine_id=1,
            matched_name="m",
            ratings_average=4.3,
            ratings_count=100,
            wine_url="https://vivino/1",
        ),
        munskankarna=MunskankarnaPayload(score=17, value_rating="fynd", review_url="https://msk/1"),
    )
    out = format_message([rw], RD, source="vivino")
    assert "[Vivino: 4\\.3 ★](https://vivino/1)" in out
    assert "[Munskänkarna: 17/20 \\(fynd\\)](https://msk/1)" in out


def test_missing_source_skips_its_row():
    rw = RankedWine(
        wine=_wine(),
        rank_score=4.0,
        vivino=VivinoPayload(
            wine_id=1,
            matched_name="m",
            ratings_average=4.0,
            ratings_count=10,
            wine_url="https://vivino/1",
        ),
        munskankarna=None,
    )
    out = format_message([rw], RD, source="vivino")
    assert "Vivino" in out
    assert "Munskänkarna:" not in out


def test_munskankarna_without_review_url_renders_plain_text():
    rw = RankedWine(
        wine=_wine(),
        rank_score=15,
        munskankarna=MunskankarnaPayload(score=15, value_rating="prisvärt", review_url=None),
    )
    out = format_message([rw], RD, source="munskankarna")
    # Plain text, not a link
    assert "Munskänkarna: 15/20" in out
    assert "(None)" not in out  # no None URL leaked
    assert "[Munskänkarna" not in out  # no link wrapper


def test_header_shows_ranking_source():
    rw = RankedWine(
        wine=_wine(),
        rank_score=17,
        munskankarna=MunskankarnaPayload(score=17, review_url="https://m/1"),
    )
    assert "Rankas av Munskänkarna" in format_message([rw], RD, source="munskankarna")
    assert "Rankas av Vivino" in format_message([rw], RD, source="vivino")


def test_budget_filter_drops_expensive_wines():
    cheap = RankedWine(
        wine=_wine("1", "Cheap", price=100),
        rank_score=10,
        munskankarna=MunskankarnaPayload(score=10, review_url="https://m/1"),
    )
    pricey = RankedWine(
        wine=_wine("2", "Pricey", price=999),
        rank_score=20,
        munskankarna=MunskankarnaPayload(score=20, review_url="https://m/2"),
    )
    out = format_message([pricey, cheap], RD, source="munskankarna", max_price=500)
    assert "Cheap" in out
    assert "Pricey" not in out
    assert "Budget: 500 kr" in out


def test_markdownv2_escapes_dots_and_parens():
    rw = RankedWine(
        wine=_wine(name="Wine 4.5", price=100),
        rank_score=4.5,
        vivino=VivinoPayload(
            wine_id=1,
            matched_name="m",
            ratings_average=4.5,
            ratings_count=10,
            wine_url="https://v/1",
        ),
    )
    out = format_message([rw], RD, source="vivino")
    assert "4\\.5" in out
    assert "Wine 4\\.5" in out


def test_format_message_backfill_notice():
    rw = RankedWine(
        wine=_wine(name="X", price=100),
        rank_score=10,
        munskankarna=MunskankarnaPayload(score=10, review_url="https://m/1"),
    )
    plain = format_message([rw], RD, source="munskankarna")
    backfilled = format_message([rw], RD, source="munskankarna", is_backfill=True)

    assert "Försenad publicering" not in plain
    assert "Försenad publicering" in backfilled
    # Notice appears before the title
    assert backfilled.index("Försenad publicering") < backfilled.index("Tillfälligt sortiment")
