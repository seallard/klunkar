from datetime import date
from unittest.mock import MagicMock

from klunkar import ranking
from klunkar.bot import parse_category_args
from klunkar.models import MunskankarnaPayload, RankedWine, Wine
from klunkar.release import format_message

RD = date(2026, 4, 24)


def _wine(num, name="W", price=200.0):
    return Wine(
        sb_product_number=num,
        sb_product_id=num,
        release_date=RD,
        name=name,
        producer="P",
        sb_url=f"https://sb/{num}",
        price=price,
        wine_type="Rött vin",
    )


# ---- parse_category_args -------------------------------------------------


def test_parse_simple():
    assert parse_category_args("fynd") == (["fynd"], [])


def test_parse_multi_csv():
    resolved, unknown = parse_category_args("fynd,prisvärt")
    assert resolved == ["fynd", "prisvärt"]
    assert unknown == []


def test_parse_aliases_and_dedupe():
    resolved, unknown = parse_category_args("mer, prisv,prisvart, fynd")
    assert resolved == ["mer än prisvärt", "prisvärt", "fynd"]
    assert unknown == []


def test_parse_empty_returns_empty():
    assert parse_category_args("") == ([], [])
    assert parse_category_args("   ") == ([], [])


def test_parse_clear_word_is_unknown():
    """`clear` no longer special-cased; it lands in unknown like any other token."""
    resolved, unknown = parse_category_args("clear")
    assert resolved == []
    assert unknown == ["clear"]


def test_parse_unknown_collected():
    resolved, unknown = parse_category_args("fynd,nonsens,whatever")
    assert resolved == ["fynd"]
    assert unknown == ["nonsens", "whatever"]


def test_parse_case_insensitive():
    assert parse_category_args("FYND") == (["fynd"], [])


# ---- ranking with value_ratings filter -----------------------------------


def _vivino(avg=4.0, count=100):
    return {
        "wine_id": 1,
        "matched_name": "m",
        "ratings_average": avg,
        "ratings_count": count,
        "wine_url": "https://v/1",
    }


def _msk(score, value):
    return {"score": score, "value_rating": value, "tasting_note": "n", "review_url": "https://m/1"}


def test_value_ratings_filter_drops_non_matches(monkeypatch):
    rows = [
        (_wine("1"), {"munskankarna": _msk(15, "fynd")}),
        (_wine("2"), {"munskankarna": _msk(17, "ej prisvärt")}),
        (_wine("3"), {"munskankarna": _msk(14, "prisvärt")}),
    ]
    monkeypatch.setattr(ranking.db, "get_wines_with_enrichments", lambda c, d: rows)
    out = ranking.build_ranked_view(
        MagicMock(),
        RD,
        source="munskankarna",
        value_ratings={"fynd", "prisvärt"},
    )
    assert {r.wine.sb_product_number for r in out} == {"1", "3"}


def test_value_ratings_filter_excludes_wines_without_munskankarna(monkeypatch):
    """When ranked by Vivino with category=fynd, only wines with both qualify."""
    rows = [
        (
            _wine("1"),
            {
                "vivino": _vivino(4.5, 200),
                "munskankarna": _msk(17, "fynd"),
            },
        ),
        (_wine("2"), {"vivino": _vivino(4.3, 100)}),  # no munskankarna
        (
            _wine("3"),
            {
                "vivino": _vivino(4.2, 50),
                "munskankarna": _msk(14, "prisvärt"),
            },
        ),
    ]
    monkeypatch.setattr(ranking.db, "get_wines_with_enrichments", lambda c, d: rows)
    out = ranking.build_ranked_view(
        MagicMock(),
        RD,
        source="vivino",
        value_ratings={"fynd"},
    )
    assert [r.wine.sb_product_number for r in out] == ["1"]


def test_value_ratings_none_means_no_filter(monkeypatch):
    rows = [
        (_wine("1"), {"munskankarna": _msk(15, "fynd")}),
        (_wine("2"), {"munskankarna": _msk(17, "ej prisvärt")}),
    ]
    monkeypatch.setattr(ranking.db, "get_wines_with_enrichments", lambda c, d: rows)
    out = ranking.build_ranked_view(MagicMock(), RD, source="munskankarna", value_ratings=None)
    assert len(out) == 2


# ---- format_message header shows the filter ------------------------------


def test_format_message_shows_active_filter():
    rw = RankedWine(
        wine=_wine("1", "X"),
        rank_score=15,
        munskankarna=MunskankarnaPayload(score=15, value_rating="fynd", review_url="https://m/1"),
    )
    out = format_message(
        [rw],
        RD,
        source="munskankarna",
        value_ratings={"fynd", "prisvärt"},
    )
    assert "Prisvärdhet: fynd, prisvärt" in out


def test_format_message_omits_filter_when_none():
    rw = RankedWine(
        wine=_wine("1", "X"),
        rank_score=15,
        munskankarna=MunskankarnaPayload(score=15, value_rating="fynd", review_url="https://m/1"),
    )
    out = format_message([rw], RD, source="munskankarna")
    assert "Prisvärdhet" not in out
