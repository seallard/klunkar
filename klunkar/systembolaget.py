import logging
import re
import unicodedata
from datetime import date

import httpx
from pydantic import BaseModel, ConfigDict

log = logging.getLogger(__name__)

_APIM_HEADER = "Ocp-Apim-Subscription-Key"
_SEARCH_URL = "https://api-extern.systembolaget.se/sb-api-ecommerce/v1/productsearch/search"
_SB_PRODUCT_URL = "https://www.systembolaget.se/produkt/vin/{name_slug}-{product_id}/"
_CALENDAR_URL = "https://www.systembolaget.se/nytt/om-vara-nyheter/lanseringar/"

# Chunk of Systembolaget's Next.js JS to extract APIM key from
_APIM_KEY_RE = re.compile(r"['\"]([0-9a-f]{32})['\"]")
_NEXT_CHUNK_RE = re.compile(r'src="(/_next/static/chunks/[^"]+\.js)"')

_HREF_DATE_RE = re.compile(
    r"/sortiment/tillfalligt-sortiment/\?[^\"']*saljstart-fran=(\d{4}-\d{2}-\d{2})"
)


class SBProduct(BaseModel):
    model_config = ConfigDict(frozen=True)

    product_id: str
    product_number: str
    name: str
    producer: str
    product_url: str
    price: float
    wine_type: str
    country: str = ""


def _extract_apim_key_from_js(js: str) -> str | None:
    """Return the first 32-hex literal that looks like a real key.

    Skips low-entropy strings (e.g. "00000000…", "ffffffff…"), which are
    placeholders some Next.js chunks carry alongside the real key.
    """
    for match in _APIM_KEY_RE.findall(js):
        if len(set(match)) > 4:
            return match
    return None


def scrape_apim_key(client: httpx.Client) -> str:
    """Fetch Systembolaget's public JS bundle and extract the APIM key."""
    r = client.get("https://www.systembolaget.se/sortiment/vin/", follow_redirects=True)
    r.raise_for_status()
    chunk_paths = _NEXT_CHUNK_RE.findall(r.text)
    for path in chunk_paths:
        js_r = client.get(f"https://www.systembolaget.se{path}")
        if js_r.status_code != 200:
            continue
        key = _extract_apim_key_from_js(js_r.text)
        if key:
            log.info("Scraped APIM key: %s…", key[:8])
            return key
    raise RuntimeError("Could not scrape APIM key from Systembolaget JS bundles")


def scrape_release_dates(client: httpx.Client) -> list[date]:
    """Scrape upcoming tillfälligt sortiment release dates from the official calendar page."""
    r = client.get(_CALENDAR_URL, follow_redirects=True)
    r.raise_for_status()
    html = r.text

    dates: set[date] = set()

    for m in _HREF_DATE_RE.finditer(html):
        dates.add(date.fromisoformat(m.group(1)))

    return sorted(d for d in dates if d >= date.today())


def _headers(apim_key: str) -> dict[str, str]:
    return {_APIM_HEADER: apim_key, "Accept": "application/json"}


def _name_slug(product: dict) -> str:
    name = (product.get("productNameBold") or "").strip()
    nfkd = unicodedata.normalize("NFKD", name)
    ascii_str = nfkd.encode("ascii", "ignore").decode()
    cleaned = re.sub(r"[^a-z0-9\s-]", "", ascii_str.lower())
    return re.sub(r"\s+", "-", cleaned.strip())


def _product_url(product: dict) -> str:
    number = product.get("productNumber") or product["productId"]
    return _SB_PRODUCT_URL.format(name_slug=_name_slug(product), product_id=number)


def _parse_product(p: dict) -> SBProduct:
    # `productNumberShort` is the bare artikelnummer (e.g. "91176") that appears
    # on receipts, in the SB app, and on Munskänkarna's pages. `productNumber`
    # ("9117601") embeds a 2-digit pack-size suffix and is only useful for URLs.
    artikelnummer = str(p.get("productNumberShort") or p.get("productNumber") or p["productId"])
    return SBProduct(
        product_id=p["productId"],
        product_number=artikelnummer,
        name=((p.get("productNameBold") or "") + " " + (p.get("productNameThin") or "")).strip(),
        producer=p.get("producerName", ""),
        product_url=_product_url(p),
        price=p.get("price", 0.0),
        wine_type=p.get("categoryLevel2", ""),
        country=p.get("country", "") or "",
    )


def fetch_release_products(
    release_date: date,
    apim_key: str,
    client: httpx.Client,
) -> list[SBProduct]:
    date_str = release_date.isoformat()
    params = {
        "assortmentText": "Tillfälligt sortiment",
        "categoryLevel1": "Vin",
        "page": 1,
    }

    products: list[SBProduct] = []
    total_pages = 1

    while params["page"] <= total_pages:
        r = client.get(_SEARCH_URL, params=params, headers=_headers(apim_key))
        if r.status_code == 401:
            raise PermissionError("APIM key rejected (401)")
        r.raise_for_status()
        data = r.json()
        meta = data.get("metadata", {})
        total_pages = meta.get("totalPages", 1)
        for p in data.get("products", []):
            if (p.get("productLaunchDate") or "")[:10] == date_str:
                products.append(_parse_product(p))
        params["page"] += 1

    log.info("Fetched %d products for %s", len(products), date_str)
    return products
