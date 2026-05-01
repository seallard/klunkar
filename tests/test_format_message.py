from datetime import date

from klunkar.models import MunskankarnaPayload, RankedWine, VivinoPayload, Wine
from klunkar.release import format_message

RD = date(2026, 4, 24)


def _wine(num="1", name="Foo", price=200.0):
    return Wine(
        sb_product_number=num, sb_product_id=num, release_date=RD,
        name=name, producer="P", sb_url=f"https://sb.se/{num}",
        price=price, wine_type="Rött vin",
    )


def test_vivino_message_uses_vivino_link_and_omits_vivino_in_link_row():
    rw = RankedWine(
        wine=_wine(),
        rank_score=4.0,
        vivino=VivinoPayload(wine_id=1, matched_name="m", ratings_average=4.3,
                             ratings_count=100, wine_url="https://vivino/1"),
        munskankarna=MunskankarnaPayload(score=17, value_rating="fynd",
                                         review_url="https://msk/1"),
    )
    out = format_message([rw], RD, source="vivino")
    assert "Rankas av Vivino" in out
    assert "(https://vivino/1)" in out                  # primary link
    assert "[Munskänkarna](https://msk/1)" in out       # complementary link
    assert "[Vivino](https://vivino/1)" not in out      # don't double-link primary


def test_munskankarna_message_uses_msk_link_and_shows_value_rating():
    rw = RankedWine(
        wine=_wine(),
        rank_score=17,
        vivino=None,
        munskankarna=MunskankarnaPayload(score=17, value_rating="fynd",
                                         review_url="https://msk/1"),
    )
    out = format_message([rw], RD, source="munskankarna")
    assert "Rankas av Munskänkarna" in out
    assert "(https://msk/1)" in out
    assert "17/20 Munskänkarna" in out
    assert "fynd" in out


def test_budget_filter_drops_expensive_wines():
    cheap = RankedWine(
        wine=_wine("1", "Cheap", price=100), rank_score=10,
        munskankarna=MunskankarnaPayload(score=10, review_url="https://m/1"),
    )
    pricey = RankedWine(
        wine=_wine("2", "Pricey", price=999), rank_score=20,
        munskankarna=MunskankarnaPayload(score=20, review_url="https://m/2"),
    )
    out = format_message([pricey, cheap], RD, source="munskankarna", max_price=500)
    assert "Cheap" in out
    assert "Pricey" not in out
    assert "max 500 kr" in out


def test_markdownv2_escapes_dots_and_parens():
    rw = RankedWine(
        wine=_wine(name="Wine 4.5", price=100), rank_score=4.5,
        vivino=VivinoPayload(wine_id=1, matched_name="m", ratings_average=4.5,
                             ratings_count=10, wine_url="https://v/1"),
    )
    out = format_message([rw], RD, source="vivino")
    assert "4\\.5" in out                    # decimal escaped
    assert "Wine 4\\.5" in out                # name escape
