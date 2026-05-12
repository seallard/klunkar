from datetime import date

from klunkar.models import RankedWine, Source, Wine
from klunkar.release import format_message
from klunkar.sources.munskankarna import MunskankarnaPayload
from klunkar.sources.vinbanken import VinbankenPayload
from klunkar.sources.vivino import VivinoPayload

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


def _ranked(wine, rank_score=0.0, **payloads_by_name):
    return RankedWine(
        wine=wine,
        rank_score=rank_score,
        payloads={Source(k): v for k, v in payloads_by_name.items() if v is not None},
    )


def test_title_row_is_just_glass_and_name():
    rw = _ranked(
        _wine(name="Foo", price=149),
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
    assert "🍷 Foo —" not in out
    assert "🥇" not in out and "🥈" not in out and "🥉" not in out


def test_systembolaget_renders_as_its_own_labeled_row():
    rw = _ranked(_wine(num="1", name="Foo", price=149), rank_score=4.0)
    out = format_message([rw], RD, source="vivino")
    assert "[Systembolaget: 149 kr](https://sb.se/1)" in out


def test_systembolaget_row_when_price_missing():
    rw = _ranked(_wine(num="1", name="Foo", price=None), rank_score=4.0)
    out = format_message([rw], RD, source="vivino")
    assert "[Systembolaget: köp](https://sb.se/1)" in out


def test_each_present_source_renders_its_own_row_with_link():
    rw = _ranked(
        _wine(),
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
    rw = _ranked(
        _wine(),
        rank_score=4.0,
        vivino=VivinoPayload(
            wine_id=1,
            matched_name="m",
            ratings_average=4.0,
            ratings_count=10,
            wine_url="https://vivino/1",
        ),
    )
    out = format_message([rw], RD, source="vivino")
    assert "Vivino" in out
    assert "Munskänkarna:" not in out


def test_vinbanken_row_renders_score_and_url():
    rw = _ranked(
        _wine(),
        rank_score=92,
        vinbanken=VinbankenPayload(score=92, review_url="https://vb/1"),
    )
    out = format_message([rw], RD, source="vinbanken")
    assert "[Vinbanken: 92/100](https://vb/1)" in out


def test_vinbanken_row_renders_fynd_badge():
    rw = _ranked(
        _wine(),
        rank_score=90,
        vinbanken=VinbankenPayload(score=90, fynd=True, review_url="https://vb/1"),
    )
    out = format_message([rw], RD, source="vinbanken")
    assert "[Vinbanken: 90/100 \\(fynd\\)](https://vb/1)" in out


def test_vinbanken_without_review_url_renders_plain_text():
    rw = _ranked(
        _wine(),
        rank_score=88,
        vinbanken=VinbankenPayload(score=88, review_url=None),
    )
    out = format_message([rw], RD, source="vinbanken")
    assert "Vinbanken: 88/100" in out
    assert "[Vinbanken" not in out


def test_format_message_backfill_notice_uses_vinbanken_label():
    rw = _ranked(
        _wine(name="X", price=100),
        rank_score=92,
        vinbanken=VinbankenPayload(score=92, review_url="https://vb/1"),
    )
    out = format_message([rw], RD, source="vinbanken", is_backfill=True)
    assert "Vinbanken finns nu med" in out


def test_munskankarna_without_review_url_renders_plain_text():
    rw = _ranked(
        _wine(),
        rank_score=15,
        munskankarna=MunskankarnaPayload(score=15, value_rating="prisvärt", review_url=None),
    )
    out = format_message([rw], RD, source="munskankarna")
    assert "Munskänkarna: 15/20" in out
    assert "(None)" not in out
    assert "[Munskänkarna" not in out


def test_header_shows_ranking_source():
    rw = _ranked(
        _wine(),
        rank_score=17,
        munskankarna=MunskankarnaPayload(score=17, review_url="https://m/1"),
    )
    assert "Rankas av Munskänkarna" in format_message([rw], RD, source="munskankarna")
    assert "Rankas av Vivino" in format_message([rw], RD, source="vivino")


def test_budget_filter_drops_expensive_wines():
    cheap = _ranked(
        _wine("1", "Cheap", price=100),
        rank_score=10,
        munskankarna=MunskankarnaPayload(score=10, review_url="https://m/1"),
    )
    pricey = _ranked(
        _wine("2", "Pricey", price=999),
        rank_score=20,
        munskankarna=MunskankarnaPayload(score=20, review_url="https://m/2"),
    )
    out = format_message([pricey, cheap], RD, source="munskankarna", max_price=500)
    assert "Cheap" in out
    assert "Pricey" not in out
    assert "Budget: 500 kr" in out


def test_markdownv2_escapes_dots_and_parens():
    rw = _ranked(
        _wine(name="Wine 4.5", price=100),
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


def test_format_message_backfill_notice_names_source():
    rw = _ranked(
        _wine(name="X", price=100),
        rank_score=10,
        munskankarna=MunskankarnaPayload(score=10, review_url="https://m/1"),
    )
    plain = format_message([rw], RD, source="munskankarna")
    backfilled = format_message([rw], RD, source="munskankarna", is_backfill=True)

    assert "Uppdaterad lista" not in plain
    assert "Uppdaterad lista" in backfilled
    assert "Munskänkarna finns nu med" in backfilled
    assert backfilled.index("Uppdaterad lista") < backfilled.index("Tillfälligt sortiment")


def test_format_message_backfill_notice_uses_vivino_label():
    rw = _ranked(
        _wine(name="X", price=100),
        rank_score=4.0,
        vivino=VivinoPayload(
            wine_id=1,
            matched_name="m",
            ratings_average=4.0,
            ratings_count=10,
            wine_url="https://v/1",
        ),
    )
    out = format_message([rw], RD, source="vivino", is_backfill=True)
    assert "Vivino finns nu med" in out


def test_format_message_renders_type_counts():
    rw = _ranked(
        _wine(name="X", price=100),
        rank_score=10,
        munskankarna=MunskankarnaPayload(score=10, review_url="https://m/1"),
    )
    counts = {"Rött vin": 30, "Vitt vin": 12, "Mousserande vin": 5}
    out = format_message([rw], RD, source="munskankarna", type_counts=counts)
    assert "47 viner" in out
    assert "30 röda" in out
    assert "12 vita" in out
    assert "5 mousserande" in out


def test_format_message_lumps_uncommon_types_as_ovrigt():
    rw = _ranked(
        _wine(name="X", price=100),
        rank_score=10,
        munskankarna=MunskankarnaPayload(score=10, review_url="https://m/1"),
    )
    counts = {"Rött vin": 10, "Starkvin": 2, "Aperitifer": 1}
    out = format_message([rw], RD, source="munskankarna", type_counts=counts)
    assert "13 viner" in out
    assert "10 röda" in out
    assert "3 övrigt" in out


def test_format_message_omits_count_line_when_no_counts():
    rw = _ranked(
        _wine(name="X", price=100),
        rank_score=10,
        munskankarna=MunskankarnaPayload(score=10, review_url="https://m/1"),
    )
    out = format_message([rw], RD, source="munskankarna")
    assert "viner ·" not in out and "0 viner" not in out
