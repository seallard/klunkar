"""Regression tests for _parse_product — guards the join key against Munskänkarna."""
from klunkar import systembolaget


def _raw(**overrides):
    base = {
        "productId": "24395436",
        "productNumber": "9117601",
        "productNumberShort": "91176",
        "productNameBold": "Ca' di Pian",
        "productNameThin": "La Spinetta",
        "producerName": "La Spinetta",
        "price": 239.0,
        "categoryLevel2": "Rött vin",
    }
    base.update(overrides)
    return base


def test_parse_product_uses_productNumberShort_as_join_key():
    """The Munskänkarna join key must be the bare artikelnummer (e.g. 91176),
    not productNumber which embeds a 2-digit pack-size suffix."""
    sb = systembolaget._parse_product(_raw())
    assert sb.product_number == "91176"


def test_parse_product_falls_back_to_productNumber_when_short_missing():
    sb = systembolaget._parse_product(_raw(productNumberShort=None))
    assert sb.product_number == "9117601"


def test_parse_product_falls_back_to_productId_when_both_missing():
    sb = systembolaget._parse_product(_raw(productNumberShort=None, productNumber=None))
    assert sb.product_number == "24395436"


def test_extract_apim_key_skips_low_entropy_placeholder():
    """The first JS chunk often contains a zero-filled placeholder before the
    real key; the extractor must skip it."""
    js = '"00000000000000000000000000000000" stuff "ab12cd34ef56789012ab34cd56ef7890"'
    assert systembolaget._extract_apim_key_from_js(js) == "ab12cd34ef56789012ab34cd56ef7890"


def test_extract_apim_key_returns_none_when_only_placeholder():
    js = '"00000000000000000000000000000000" "ffffffffffffffffffffffffffffffff"'
    assert systembolaget._extract_apim_key_from_js(js) is None
