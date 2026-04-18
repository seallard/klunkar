import logging
import re
import unicodedata
from dataclasses import dataclass
from datetime import date

import httpx

log = logging.getLogger(__name__)

_APIM_HEADER = "Ocp-Apim-Subscription-Key"
_SEARCH_URL = "https://api-extern.systembolaget.se/sb-api-ecommerce/v1/productsearch/search"
_SB_PRODUCT_URL = "https://www.systembolaget.se/produkt/vin/{name_slug}-{product_id}/"

# Chunk of Systembolaget's Next.js JS to extract APIM key from
_APIM_KEY_RE = re.compile(r"['\"]([0-9a-f]{32})['\"]")
_NEXT_CHUNK_RE = re.compile(r'src="(/_next/static/chunks/[^"]+\.js)"')


@dataclass
class SBProduct:
    product_id: str
    name: str
    producer: str
    product_url: str
    price: float
    wine_type: str


def _extract_apim_key_from_js(js: str) -> str | None:
    matches = _APIM_KEY_RE.findall(js)
    return matches[0] if matches else None


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


def _headers(apim_key: str) -> dict:
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
    return SBProduct(
        product_id=p["productId"],
        name=((p.get("productNameBold") or "") + " " + (p.get("productNameThin") or "")).strip(),
        producer=p.get("producerName", ""),
        product_url=_product_url(p),
        price=p.get("price", 0.0),
        wine_type=p.get("categoryLevel2", ""),
    )


def fetch_release_products(
    release_date: date,
    apim_key: str,
    client: httpx.Client,
) -> list[SBProduct]:
    date_str = release_date.isoformat()
    params = {
        "productLaunchDate.min": date_str,
        "productLaunchDate.max": date_str,
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
            products.append(_parse_product(p))
        params["page"] += 1

    log.info("Fetched %d products for %s", len(products), date_str)
    return products


def fetch_upcoming_release_dates(
    from_date: date,
    to_date: date,
    apim_key: str,
    client: httpx.Client,
) -> list[date]:
    params = {
        "productLaunchDate.min": from_date.isoformat(),
        "productLaunchDate.max": to_date.isoformat(),
        "assortmentText": "Tillfälligt sortiment",
        "categoryLevel1": "Vin",
        "page": 1,
    }
    dates: set[date] = set()
    total_pages = 1
    while params["page"] <= total_pages:
        r = client.get(_SEARCH_URL, params=params, headers=_headers(apim_key))
        if r.status_code == 401:
            raise PermissionError("APIM key rejected (401)")
        r.raise_for_status()
        data = r.json()
        total_pages = data.get("metadata", {}).get("totalPages", 1)
        for p in data.get("products", []):
            launch = p.get("productLaunchDate")
            if launch:
                d = date.fromisoformat(launch[:10])
                if from_date <= d <= to_date:
                    dates.add(d)
        params["page"] += 1
    return sorted(dates)


def has_release(release_date: date, apim_key: str, client: httpx.Client) -> bool:
    date_str = release_date.isoformat()
    params = {
        "productLaunchDate.min": date_str,
        "productLaunchDate.max": date_str,
        "assortmentText": "Tillfälligt sortiment",
        "categoryLevel1": "Vin",
        "page": 1,
    }
    r = client.get(_SEARCH_URL, params=params, headers=_headers(apim_key))
    if r.status_code == 401:
        raise PermissionError("APIM key rejected (401)")
    r.raise_for_status()
    return r.json().get("metadata", {}).get("docCount", 0) > 0
