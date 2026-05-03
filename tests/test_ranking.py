from datetime import date
from unittest.mock import MagicMock

from klunkar import ranking
from klunkar.models import Wine

RD = date(2026, 4, 24)


def _wine(num, name, price=200.0, wine_type="Rött vin"):
    return Wine(
        sb_product_number=num,
        sb_product_id=num,
        release_date=RD,
        name=name,
        producer="X",
        sb_url=f"https://sb/{num}",
        price=price,
        wine_type=wine_type,
    )


def _vivino(avg, count, name="match"):
    return {
        "wine_id": 1,
        "matched_name": name,
        "ratings_average": avg,
        "ratings_count": count,
        "wine_url": "https://v/1",
    }


def _munskankarna(score, value=None):
    return {"score": score, "value_rating": value, "tasting_note": "n", "review_url": "https://m/1"}


def _conn_with(rows):
    """rows = [(Wine, {source: payload})]; mocks db.get_wines_with_enrichments via patch."""
    conn = MagicMock()
    return conn, rows


def test_vivino_filter_excludes_non_matched(monkeypatch):
    rows = [
        (_wine("1", "A"), {"vivino": _vivino(4.5, 100)}),
        (_wine("2", "B"), {"munskankarna": _munskankarna(18)}),
        (_wine("3", "C"), {}),
    ]
    monkeypatch.setattr(ranking.db, "get_wines_with_enrichments", lambda c, d: rows)
    out = ranking.build_ranked_view(MagicMock(), RD, source="vivino")
    assert [r.wine.sb_product_number for r in out] == ["1"]


def test_munskankarna_orders_by_score(monkeypatch):
    rows = [
        (_wine("1", "A"), {"munskankarna": _munskankarna(14.5, "fynd")}),
        (_wine("2", "B"), {"munskankarna": _munskankarna(17.0, "ej prisvärt")}),
        (_wine("3", "C"), {"munskankarna": _munskankarna(15.0, "prisvärt")}),
    ]
    monkeypatch.setattr(ranking.db, "get_wines_with_enrichments", lambda c, d: rows)
    out = ranking.build_ranked_view(MagicMock(), RD, source="munskankarna")
    assert [r.wine.sb_product_number for r in out] == ["2", "3", "1"]


def test_munskankarna_tiebreak_value_then_price(monkeypatch):
    rows = [
        (_wine("a", "A", price=300), {"munskankarna": _munskankarna(15.0, "prisvärt")}),
        (_wine("b", "B", price=500), {"munskankarna": _munskankarna(15.0, "fynd")}),
        (_wine("c", "C", price=100), {"munskankarna": _munskankarna(15.0, "prisvärt")}),
    ]
    monkeypatch.setattr(ranking.db, "get_wines_with_enrichments", lambda c, d: rows)
    out = ranking.build_ranked_view(MagicMock(), RD, source="munskankarna")
    # fynd > prisvärt, then lower price wins
    assert [r.wine.sb_product_number for r in out] == ["b", "c", "a"]


def test_vivino_bayesian_favors_more_ratings(monkeypatch):
    # Two wines, same average rating; one has many ratings, one has few.
    # Bayesian shrinks low-count wines toward the global mean.
    rows = [
        (_wine("low", "Low"), {"vivino": _vivino(4.5, 5)}),
        (_wine("high", "High"), {"vivino": _vivino(4.5, 1000)}),
        (_wine("avg", "Avg"), {"vivino": _vivino(3.5, 1000)}),
    ]
    monkeypatch.setattr(ranking.db, "get_wines_with_enrichments", lambda c, d: rows)
    out = ranking.build_ranked_view(MagicMock(), RD, source="vivino")
    # High-count 4.5 should rank above low-count 4.5
    order = [r.wine.sb_product_number for r in out]
    assert order.index("high") < order.index("low")


def test_payloads_round_trip_to_models(monkeypatch):
    rows = [
        (
            _wine("1", "A"),
            {
                "vivino": _vivino(4.0, 50),
                "munskankarna": _munskankarna(14.0, "prisvärt"),
            },
        ),
    ]
    monkeypatch.setattr(ranking.db, "get_wines_with_enrichments", lambda c, d: rows)
    out = ranking.build_ranked_view(MagicMock(), RD, source="vivino")
    assert out[0].vivino is not None and out[0].vivino.ratings_count == 50
    assert out[0].munskankarna is not None and out[0].munskankarna.value_rating == "prisvärt"


def test_wine_types_filter_drops_other_types(monkeypatch):
    rows = [
        (_wine("1", "Red", wine_type="Rött vin"), {"vivino": _vivino(4.5, 100)}),
        (_wine("2", "White", wine_type="Vitt vin"), {"vivino": _vivino(4.3, 100)}),
        (_wine("3", "Sparkling", wine_type="Mousserande vin"), {"vivino": _vivino(4.4, 100)}),
    ]
    monkeypatch.setattr(ranking.db, "get_wines_with_enrichments", lambda c, d: rows)
    out = ranking.build_ranked_view(
        MagicMock(), RD, source="vivino", wine_types={"Rött vin", "Vitt vin"}
    )
    assert {r.wine.sb_product_number for r in out} == {"1", "2"}


def test_wine_types_none_means_no_filter(monkeypatch):
    rows = [
        (_wine("1", "Red", wine_type="Rött vin"), {"vivino": _vivino(4.5, 100)}),
        (_wine("2", "White", wine_type="Vitt vin"), {"vivino": _vivino(4.3, 100)}),
    ]
    monkeypatch.setattr(ranking.db, "get_wines_with_enrichments", lambda c, d: rows)
    out = ranking.build_ranked_view(MagicMock(), RD, source="vivino", wine_types=None)
    assert len(out) == 2


def _wine_country(num, name, country):
    return Wine(
        sb_product_number=num,
        sb_product_id=num,
        release_date=RD,
        name=name,
        producer="X",
        sb_url=f"https://sb/{num}",
        price=200.0,
        wine_type="Rött vin",
        country=country,
    )


def test_countries_filter_drops_other_countries(monkeypatch):
    rows = [
        (_wine_country("1", "It", "Italien"), {"vivino": _vivino(4.5, 100)}),
        (_wine_country("2", "Fr", "Frankrike"), {"vivino": _vivino(4.3, 100)}),
        (_wine_country("3", "Sp", "Spanien"), {"vivino": _vivino(4.4, 100)}),
    ]
    monkeypatch.setattr(ranking.db, "get_wines_with_enrichments", lambda c, d: rows)
    out = ranking.build_ranked_view(
        MagicMock(), RD, source="vivino", countries={"Italien", "Frankrike"}
    )
    assert {r.wine.sb_product_number for r in out} == {"1", "2"}
