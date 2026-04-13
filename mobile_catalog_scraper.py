#!/usr/bin/env python3
"""Flipkart mobile and smartphone catalog scraper.

This scraper avoids blocked search pages by discovering product URLs from
Flipkart product sitemap indexes, then validating each product page and writing
mobile and smartphone rows to CSV.

Use this only in ways allowed by Flipkart Terms, robots rules, and local law.
"""

from __future__ import annotations

import argparse
import csv
from collections import deque
import gzip
import hashlib
import json
import random
import re
import time
import xml.etree.ElementTree as ET
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable
from urllib.parse import parse_qs, urlencode, urljoin, urlparse, urlunparse

import requests
from bs4 import BeautifulSoup


SITEMAP_NS = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}
DEFAULT_SITEMAP_INDEXES = [
    f"https://www.flipkart.com/sitemap_pi_product_index_v2_{idx}.xml"
    for idx in range(1, 7)
]

DEFAULT_SEED_URLS = [
    "https://www.flipkart.com/motorola-g96-5g-pantone-dresden-blue-128-gb/p/itm3d5ad13991fdc",
]

DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept-Language": "en-IN,en;q=0.9",
}

MOBILE_CATEGORY_HINTS = {
    "mobile",
    "mobiles",
    "smartphone",
    "smart phone",
    "handset",
}

EXCLUDE_CATEGORY_HINTS = {
    "mobileprotection",
    "mobile accessory",
    "case",
    "cover",
    "charger",
    "cable",
    "earphone",
    "headphone",
    "watch",
    "smartwatch",
}

EXCLUDE_TITLE_HINTS = {
    "back cover",
    "cover for",
    "phone case",
    "tempered glass",
    "screen guard",
    "charger",
    "charging cable",
    "usb cable",
    "adapter",
    "earbuds",
    "neckband",
    "headphone",
    "smartwatch",
    "watch strap",
    "mobile skin",
    "holder",
    "tripod",
}

URL_INCLUDE_HINTS = {
    "iphone",
    "samsung",
    "galaxy",
    "motorola",
    "moto",
    "vivo",
    "oppo",
    "realme",
    "xiaomi",
    "redmi",
    "poco",
    "oneplus",
    "infinix",
    "tecno",
    "iqoo",
    "nokia",
    "lava",
    "itel",
    "honor",
    "nothing",
    "pixel",
    "mobile",
    "smartphone",
    "5g",
}

URL_EXCLUDE_HINTS = {
    "back-cover",
    "mobile-cover",
    "cover-",
    "case-",
    "tempered",
    "screen-guard",
    "charger",
    "cable",
    "adapter",
    "earphone",
    "earbud",
    "headphone",
    "neckband",
    "smartwatch",
    "watch",
    "tripod",
    "power-bank",
    "holder",
    "skin",
}

PHONE_BRAND_HINTS = {
    "iphone",
    "samsung",
    "motorola",
    "moto",
    "vivo",
    "oppo",
    "realme",
    "xiaomi",
    "redmi",
    "poco",
    "oneplus",
    "infinix",
    "tecno",
    "iqoo",
    "nokia",
    "lava",
    "itel",
    "honor",
    "nothing",
    "pixel",
    "hmd",
}


@dataclass
class CandidateURL:
    product_url: str
    source_sitemap: str
    sitemap_lastmod: str


@dataclass
class ProductRow:
    product_id: str
    product_url: str
    title: str
    price: str
    currency: str
    rating: str
    review_count: str
    category: str
    super_category: str
    sub_category: str
    source_sitemap: str
    sitemap_lastmod: str
    scraped_at: str


@dataclass
class ParsedProduct:
    product_id: str
    product_url: str
    title: str
    price: str
    currency: str
    rating: str
    review_count: str
    category: str
    super_category: str
    sub_category: str
    is_mobile: bool


def normalize_space(value: str) -> str:
    return re.sub(r"\s+", " ", (value or "").strip())


def parse_header_pair(value: str) -> tuple[str, str]:
    if ":" not in value:
        raise ValueError(f"Invalid header '{value}'. Expected format: Key: Value")
    key, raw = value.split(":", 1)
    key = key.strip()
    raw = raw.strip()
    if not key:
        raise ValueError(f"Invalid header '{value}'. Header key is empty")
    return key, raw


def maybe_decompress_xml(content: bytes, url: str) -> bytes:
    if url.lower().endswith(".gz"):
        return gzip.decompress(content)
    return content


def get_with_retry(
    session: requests.Session,
    url: str,
    timeout: int,
    max_retries: int,
    backoff_base: float,
) -> requests.Response:
    for attempt in range(1, max_retries + 1):
        try:
            response = session.get(url, timeout=timeout)
            if response.status_code in {429, 503}:
                raise requests.HTTPError(f"HTTP {response.status_code}")
            return response
        except requests.RequestException:
            if attempt == max_retries:
                raise
            sleep_for = backoff_base * (2 ** (attempt - 1)) + random.uniform(0.15, 0.7)
            time.sleep(sleep_for)
    raise RuntimeError("Unreachable retry state")


def get_product_page_with_fallback(
    session: requests.Session,
    url: str,
    timeout: int,
    max_retries: int,
    backoff_base: float,
) -> requests.Response:
    response = get_with_retry(
        session=session,
        url=url,
        timeout=timeout,
        max_retries=max_retries,
        backoff_base=backoff_base,
    )

    if response.status_code != 403:
        return response

    # Fallback request with minimal browser-like headers can bypass occasional
    # anti-bot responses that trigger on richer header sets.
    minimal_headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept-Language": "en-IN,en;q=0.9",
    }
    return requests.get(url, headers=minimal_headers, timeout=timeout)


def fetch_xml_bytes(
    session: requests.Session,
    url: str,
    timeout: int,
    max_retries: int,
    backoff_base: float,
) -> bytes:
    response = get_with_retry(
        session=session,
        url=url,
        timeout=timeout,
        max_retries=max_retries,
        backoff_base=backoff_base,
    )
    response.raise_for_status()
    return maybe_decompress_xml(response.content, url)


def parse_sitemap_index(xml_bytes: bytes) -> list[tuple[str, str]]:
    root = ET.fromstring(xml_bytes)
    rows: list[tuple[str, str]] = []
    for sitemap_node in root.findall("sm:sitemap", SITEMAP_NS):
        loc_node = sitemap_node.find("sm:loc", SITEMAP_NS)
        if loc_node is None or not (loc_node.text or "").strip():
            continue
        lastmod_node = sitemap_node.find("sm:lastmod", SITEMAP_NS)
        rows.append(((loc_node.text or "").strip(), normalize_space(lastmod_node.text or "")))
    return rows


def parse_urlset(xml_bytes: bytes) -> list[str]:
    root = ET.fromstring(xml_bytes)
    urls: list[str] = []
    for url_node in root.findall("sm:url", SITEMAP_NS):
        loc_node = url_node.find("sm:loc", SITEMAP_NS)
        if loc_node is None:
            continue
        loc = normalize_space(loc_node.text or "")
        if loc:
            urls.append(loc)
    return urls


def canonical_product_url(url: str) -> str:
    parsed = urlparse(url.strip())
    path = parsed.path or "/"

    # Product sitemaps often include locale-specific variants such as /hi/.
    if path.startswith("/hi/"):
        path = path[3:]
        if not path.startswith("/"):
            path = "/" + path

    if path.endswith("/") and len(path) > 1:
        path = path.rstrip("/")

    return urlunparse(("https", parsed.netloc.lower(), path, "", "", ""))


def with_page_param(url: str, page: int) -> str:
    parsed = urlparse(url)
    query = parse_qs(parsed.query)
    query["page"] = [str(page)]
    query_str = urlencode({k: v[0] for k, v in query.items()}, doseq=False)
    return urlunparse((parsed.scheme or "https", parsed.netloc, parsed.path, "", query_str, ""))


def extract_product_id(url: str) -> str:
    parsed = urlparse(url)
    query = parse_qs(parsed.query)
    pid_values = query.get("pid")
    if pid_values:
        return pid_values[0]

    m = re.search(r"/p/([^/?#]+)", parsed.path)
    if m:
        return m.group(1)

    m = re.search(r"\b([A-Z0-9]{10,20})\b", url)
    if m:
        return m.group(1)
    return ""


def looks_like_phone_candidate_url(url: str) -> bool:
    slug = urlparse(url).path.lower()
    if "/p/" not in slug:
        return False
    if any(token in slug for token in URL_EXCLUDE_HINTS):
        return False
    return any(token in slug for token in URL_INCLUDE_HINTS)


def extract_first_jsonld_product(soup: BeautifulSoup) -> dict[str, object]:
    for script in soup.find_all("script", attrs={"type": "application/ld+json"}):
        raw = (script.string or script.get_text() or "").strip()
        if not raw:
            continue
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            continue
        nodes = data if isinstance(data, list) else [data]
        for node in nodes:
            if isinstance(node, dict) and node.get("@type") == "Product":
                return node
    return {}


def extract_first_match(text: str, key: str) -> str:
    pattern = rf'"{re.escape(key)}"\s*:\s*"([^"]+)"'
    matches = re.findall(pattern, text, flags=re.IGNORECASE)
    for match in matches:
        value = normalize_space(match)
        if value and value.lower() not in {"null", "none", "na"}:
            return value
    return ""


def classify_mobile_product(title: str, category: str, super_category: str, sub_category: str) -> bool:
    category_blob = " ".join(x for x in [category, super_category, sub_category] if x).lower()
    title_blob = normalize_space(title).lower()

    if any(hint in category_blob for hint in EXCLUDE_CATEGORY_HINTS):
        return False
    if any(hint in title_blob for hint in EXCLUDE_TITLE_HINTS):
        return False

    if any(hint in category_blob for hint in MOBILE_CATEGORY_HINTS):
        return True

    if "smartphone" in title_blob or "mobile phone" in title_blob:
        return True

    if any(brand in title_blob for brand in PHONE_BRAND_HINTS):
        if any(token in title_blob for token in {"5g", " gb", "ram", "mobile", "smartphone", "iphone", "pixel"}):
            return True

    return False


def extract_product_links(html: str, base_url: str) -> list[str]:
    soup = BeautifulSoup(html, "html.parser")
    links: set[str] = set()
    for anchor in soup.select("a[href*='/p/']"):
        href = normalize_space(anchor.get("href") or "")
        if not href:
            continue
        absolute = urljoin(base_url, href)
        canonical = canonical_product_url(absolute)
        parsed = urlparse(canonical)
        if "flipkart.com" not in parsed.netloc:
            continue
        if "/p/" not in parsed.path:
            continue
        links.add(canonical)
    return sorted(links)


def load_seed_urls(seed_file: str) -> list[str]:
    path = Path(seed_file)
    if not path.exists():
        raise FileNotFoundError(f"Seed file not found: {seed_file}")

    rows: list[str] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = normalize_space(line)
        if not line or line.startswith("#"):
            continue
        rows.append(line)
    return rows


def parse_product_page(html: str, source_url: str) -> ParsedProduct:
    soup = BeautifulSoup(html, "html.parser")
    product_json = extract_first_jsonld_product(soup)

    title = ""
    if product_json:
        title = normalize_space(str(product_json.get("name", "")))

    if not title:
        title_node = soup.select_one("span.VU-ZEz, span.B_NuCI, h1")
        if title_node:
            title = normalize_space(title_node.get_text(" "))

    if not title and soup.title:
        title = normalize_space(soup.title.get_text(" "))

    canonical_url = source_url
    canonical_link = soup.find("link", attrs={"rel": "canonical"})
    if canonical_link and canonical_link.get("href"):
        canonical_url = canonical_product_url(str(canonical_link.get("href")))

    category = normalize_space(str(product_json.get("category", ""))) if product_json else ""
    super_category = extract_first_match(html, "superCategory")
    sub_category = extract_first_match(html, "subCategory")
    if not category:
        category = extract_first_match(html, "category")

    price = ""
    currency = ""
    rating = ""
    review_count = ""

    if product_json:
        offers = product_json.get("offers", {})
        if isinstance(offers, dict):
            price = normalize_space(str(offers.get("price", "")))
            currency = normalize_space(str(offers.get("priceCurrency", "")))

        aggregate = product_json.get("aggregateRating", {})
        if isinstance(aggregate, dict):
            rating = normalize_space(str(aggregate.get("ratingValue", "")))
            review_count = normalize_space(str(aggregate.get("reviewCount", "")))

    if not rating:
        rating_node = soup.select_one("div.XQDdHH, div._3LWZlK")
        if rating_node:
            rating = normalize_space(rating_node.get_text(" "))

    if not review_count:
        reviews_node = soup.select_one("span.Wphh3N, span._2_R_DZ")
        if reviews_node:
            m = re.search(r"\d[\d,]*", reviews_node.get_text(" "))
            if m:
                review_count = m.group(0).replace(",", "")

    product_id = (
        extract_product_id(canonical_url)
        or extract_first_match(html, "productId")
        or hashlib.sha256(canonical_url.encode("utf-8")).hexdigest()[:20]
    )

    is_mobile = classify_mobile_product(
        title=title,
        category=category,
        super_category=super_category,
        sub_category=sub_category,
    )

    return ParsedProduct(
        product_id=product_id,
        product_url=canonical_url,
        title=title,
        price=price,
        currency=currency,
        rating=rating,
        review_count=review_count,
        category=category,
        super_category=super_category,
        sub_category=sub_category,
        is_mobile=is_mobile,
    )


def is_challenge_page(html: str) -> bool:
    low = html.lower()
    return "recaptcha" in low and "are you a human" in low


def load_existing_ids(csv_path: Path) -> set[str]:
    if not csv_path.exists():
        return set()
    seen: set[str] = set()
    with csv_path.open("r", encoding="utf-8", newline="") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            product_id = normalize_space(row.get("product_id", ""))
            if product_id:
                seen.add(product_id)
    return seen


def iter_candidate_urls(
    session: requests.Session,
    sitemap_indexes: Iterable[str],
    search_urls: list[str],
    search_pages: int,
    seed_urls: list[str],
    seed_max_depth: int,
    discovery_mode: str,
    max_shards: int,
    max_candidates: int,
    timeout: int,
    max_retries: int,
    backoff_base: float,
    loose_url_filter: bool,
    verbose: bool,
) -> Iterable[CandidateURL]:
    seen_urls: set[str] = set()
    seen_product_ids: set[str] = set()
    yielded = 0
    shard_count = 0
    sitemap_errors = 0

    if discovery_mode in {"auto", "search"} and search_urls:
        for base_search_url in search_urls:
            for page in range(1, max(1, search_pages) + 1):
                page_url = with_page_param(base_search_url, page)
                try:
                    response = get_with_retry(
                        session=session,
                        url=page_url,
                        timeout=timeout,
                        max_retries=max_retries,
                        backoff_base=backoff_base,
                    )
                except requests.RequestException as exc:
                    if verbose:
                        print(f"Search page request failed: {page_url} ({exc})")
                    continue

                if response.status_code == 403 and is_challenge_page(response.text):
                    if verbose:
                        print(f"Search page blocked by challenge: {page_url}")
                    break

                links = extract_product_links(response.text, response.url)
                if verbose:
                    print(f"Search page {page}: found {len(links)} product links")

                if not links:
                    continue

                for linked_url in links:
                    if linked_url in seen_urls:
                        continue
                    if not loose_url_filter and not looks_like_phone_candidate_url(linked_url):
                        continue

                    product_id = extract_product_id(linked_url)
                    if product_id and product_id in seen_product_ids:
                        continue

                    seen_urls.add(linked_url)
                    if product_id:
                        seen_product_ids.add(product_id)

                    yielded += 1
                    yield CandidateURL(
                        product_url=linked_url,
                        source_sitemap=base_search_url,
                        sitemap_lastmod=f"search_page_{page}",
                    )

                    if max_candidates > 0 and yielded >= max_candidates:
                        return

        if yielded > 0:
            return

        if discovery_mode == "search":
            return

    if discovery_mode in {"auto", "sitemap"}:
        for index_url in sitemap_indexes:
            try:
                index_xml = fetch_xml_bytes(
                    session=session,
                    url=index_url,
                    timeout=timeout,
                    max_retries=max_retries,
                    backoff_base=backoff_base,
                )
            except requests.RequestException as exc:
                sitemap_errors += 1
                if verbose:
                    print(f"Skipping sitemap index due to request error: {index_url} ({exc})")
                continue

            shard_entries = parse_sitemap_index(index_xml)
            if verbose:
                print(f"Loaded index: {index_url} ({len(shard_entries)} shard refs)")

            for shard_url, shard_lastmod in shard_entries:
                if max_shards > 0 and shard_count >= max_shards:
                    return
                shard_count += 1

                try:
                    shard_xml = fetch_xml_bytes(
                        session=session,
                        url=shard_url,
                        timeout=timeout,
                        max_retries=max_retries,
                        backoff_base=backoff_base,
                    )
                except requests.RequestException as exc:
                    if verbose:
                        print(f"Skipping shard due to request error: {shard_url} ({exc})")
                    continue

                urls = parse_urlset(shard_xml)
                if verbose:
                    print(f"  Shard {shard_count}: {len(urls)} URLs")

                for raw_url in urls:
                    canonical = canonical_product_url(raw_url)
                    if canonical in seen_urls:
                        continue

                    if not loose_url_filter and not looks_like_phone_candidate_url(canonical):
                        continue

                    product_id = extract_product_id(canonical)
                    if product_id and product_id in seen_product_ids:
                        continue

                    seen_urls.add(canonical)
                    if product_id:
                        seen_product_ids.add(product_id)

                    yielded += 1
                    yield CandidateURL(
                        product_url=canonical,
                        source_sitemap=shard_url,
                        sitemap_lastmod=shard_lastmod,
                    )

                    if max_candidates > 0 and yielded >= max_candidates:
                        return

        if yielded > 0:
            return

        if discovery_mode == "sitemap":
            return

    if verbose:
        if discovery_mode == "seed":
            print("Using seed graph crawl discovery mode.")
        else:
            print(
                "Falling back to seed graph crawl because no prior candidates "
                f"were produced (sitemap_errors={sitemap_errors})."
            )

    queue: deque[tuple[str, int, str]] = deque()
    for seed in seed_urls:
        canonical_seed = canonical_product_url(seed)
        if "/p/" not in urlparse(canonical_seed).path:
            continue
        queue.append((canonical_seed, 0, "seed"))

    while queue:
        current_url, depth, source = queue.popleft()
        if current_url in seen_urls:
            continue

        if not loose_url_filter and not looks_like_phone_candidate_url(current_url):
            continue

        seen_urls.add(current_url)
        product_id = extract_product_id(current_url)
        if product_id:
            if product_id in seen_product_ids:
                continue
            seen_product_ids.add(product_id)

        yielded += 1
        yield CandidateURL(
            product_url=current_url,
            source_sitemap=source,
            sitemap_lastmod=f"seed_depth_{depth}",
        )

        if max_candidates > 0 and yielded >= max_candidates:
            return

        if depth >= seed_max_depth:
            continue

        try:
            response = get_product_page_with_fallback(
                session=session,
                url=current_url,
                timeout=timeout,
                max_retries=max_retries,
                backoff_base=backoff_base,
            )
        except requests.RequestException:
            continue

        if response.status_code != 200 or is_challenge_page(response.text):
            continue

        for linked_url in extract_product_links(response.text, response.url):
            if linked_url in seen_urls:
                continue
            if not loose_url_filter and not looks_like_phone_candidate_url(linked_url):
                continue
            queue.append((linked_url, depth + 1, current_url))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Discover and export Flipkart mobile/smartphone catalog rows to CSV"
    )
    parser.add_argument(
        "--output",
        default="data/catalog/mobile_smartphones.csv",
        help="CSV output file path",
    )
    parser.add_argument(
        "--sitemap-index",
        action="append",
        default=[],
        help="Override sitemap index URL (repeatable). Defaults to sitemap_pi_product_index_v2_1..6",
    )
    parser.add_argument(
        "--search-url",
        action="append",
        default=[],
        help="Search URL for candidate discovery (repeatable)",
    )
    parser.add_argument(
        "--search-pages",
        type=int,
        default=3,
        help="Number of pages per --search-url to crawl for candidates",
    )
    parser.add_argument(
        "--discovery-mode",
        choices=["auto", "search", "sitemap", "seed"],
        default="auto",
        help="Candidate discovery mode: search, sitemap, seed, or auto fallback",
    )
    parser.add_argument(
        "--seed-url",
        action="append",
        default=[],
        help="Seed product URL for graph crawl fallback when sitemaps are blocked (repeatable)",
    )
    parser.add_argument(
        "--seed-file",
        default="",
        help="Optional text file with one seed product URL per line",
    )
    parser.add_argument(
        "--seed-max-depth",
        type=int,
        default=3,
        help="Graph crawl depth when using seed URL fallback",
    )
    parser.add_argument(
        "--max-products",
        type=int,
        default=200,
        help="Maximum mobile/smartphone rows to write",
    )
    parser.add_argument(
        "--max-candidates",
        type=int,
        default=5000,
        help="Maximum candidate product URLs to inspect",
    )
    parser.add_argument(
        "--max-shards",
        type=int,
        default=20,
        help="Maximum product sitemap shards to read (0 = no limit)",
    )
    parser.add_argument(
        "--loose-url-filter",
        action="store_true",
        help="Do not require mobile-like keywords in URL before page fetch",
    )
    parser.add_argument(
        "--include-accessories",
        action="store_true",
        help="Write all parsed products even when mobile classification is false",
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Skip product IDs already present in the output CSV",
    )
    parser.add_argument(
        "--cookie",
        default="",
        help="Optional raw Cookie header value copied from browser/curl",
    )
    parser.add_argument(
        "--extra-header",
        action="append",
        default=[],
        help="Optional header in 'Key: Value' format (repeatable)",
    )
    parser.add_argument("--timeout", type=int, default=25, help="HTTP timeout in seconds")
    parser.add_argument("--max-retries", type=int, default=3, help="Retries per request")
    parser.add_argument("--backoff-base", type=float, default=1.4, help="Retry backoff base")
    parser.add_argument("--delay-min", type=float, default=0.4, help="Minimum delay between product requests")
    parser.add_argument("--delay-max", type=float, default=1.1, help="Maximum delay between product requests")
    parser.add_argument("--verbose", action="store_true", help="Print verbose progress logs")
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    if args.max_products <= 0:
        raise SystemExit("--max-products must be > 0")
    if args.max_candidates <= 0:
        raise SystemExit("--max-candidates must be > 0")
    if args.max_shards < 0:
        raise SystemExit("--max-shards must be >= 0")
    if args.search_pages <= 0:
        raise SystemExit("--search-pages must be > 0")
    if args.seed_max_depth < 0:
        raise SystemExit("--seed-max-depth must be >= 0")
    if args.delay_min <= 0 or args.delay_max <= 0:
        raise SystemExit("--delay-min and --delay-max must be > 0")
    if args.delay_min > args.delay_max:
        raise SystemExit("--delay-min cannot be greater than --delay-max")

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    existing_ids = load_existing_ids(output_path) if args.resume else set()

    session = requests.Session()
    session.headers.update(DEFAULT_HEADERS)

    if args.cookie:
        session.headers["Cookie"] = args.cookie

    for header in args.extra_header:
        key, value = parse_header_pair(header)
        session.headers[key] = value

    sitemap_indexes = args.sitemap_index or DEFAULT_SITEMAP_INDEXES

    seed_urls = [normalize_space(u) for u in args.seed_url if normalize_space(u)]
    if args.seed_file:
        seed_urls.extend(load_seed_urls(args.seed_file))
    if not seed_urls:
        seed_urls = list(DEFAULT_SEED_URLS)

    search_urls = [normalize_space(u) for u in args.search_url if normalize_space(u)]
    if args.discovery_mode == "search" and not search_urls:
        raise SystemExit("--search-url is required when --discovery-mode search")

    fieldnames = list(ProductRow.__dataclass_fields__.keys())
    file_exists = output_path.exists()

    scanned_candidates = 0
    written_rows = 0
    skipped_non_mobile = 0
    blocked_pages = 0
    failed_pages = 0

    with output_path.open("a", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        if not file_exists:
            writer.writeheader()

        for candidate in iter_candidate_urls(
            session=session,
            sitemap_indexes=sitemap_indexes,
            search_urls=search_urls,
            search_pages=args.search_pages,
            seed_urls=seed_urls,
            seed_max_depth=args.seed_max_depth,
            discovery_mode=args.discovery_mode,
            max_shards=args.max_shards,
            max_candidates=args.max_candidates,
            timeout=args.timeout,
            max_retries=args.max_retries,
            backoff_base=args.backoff_base,
            loose_url_filter=args.loose_url_filter,
            verbose=args.verbose,
        ):
            scanned_candidates += 1

            try:
                response = get_product_page_with_fallback(
                    session=session,
                    url=candidate.product_url,
                    timeout=args.timeout,
                    max_retries=args.max_retries,
                    backoff_base=args.backoff_base,
                )
            except requests.RequestException:
                failed_pages += 1
                continue

            if response.status_code == 404:
                continue

            if response.status_code == 403 and is_challenge_page(response.text):
                blocked_pages += 1
                continue

            try:
                parsed = parse_product_page(response.text, candidate.product_url)
            except Exception:
                failed_pages += 1
                continue

            if parsed.product_id in existing_ids:
                continue

            if not args.include_accessories and not parsed.is_mobile:
                skipped_non_mobile += 1
                continue

            row = ProductRow(
                product_id=parsed.product_id,
                product_url=parsed.product_url,
                title=parsed.title,
                price=parsed.price,
                currency=parsed.currency,
                rating=parsed.rating,
                review_count=parsed.review_count,
                category=parsed.category,
                super_category=parsed.super_category,
                sub_category=parsed.sub_category,
                source_sitemap=candidate.source_sitemap,
                sitemap_lastmod=candidate.sitemap_lastmod,
                scraped_at=datetime.now(timezone.utc).isoformat(),
            )
            writer.writerow(asdict(row))
            fh.flush()

            existing_ids.add(parsed.product_id)
            written_rows += 1

            if written_rows % 20 == 0:
                print(
                    f"Progress: wrote={written_rows}, scanned={scanned_candidates}, "
                    f"skipped_non_mobile={skipped_non_mobile}, blocked={blocked_pages}"
                )

            if written_rows >= args.max_products:
                break

            time.sleep(random.uniform(args.delay_min, args.delay_max))

    print(
        "Done. "
        f"wrote={written_rows}, "
        f"scanned_candidates={scanned_candidates}, "
        f"skipped_non_mobile={skipped_non_mobile}, "
        f"blocked_pages={blocked_pages}, "
        f"failed_pages={failed_pages}, "
        f"output={output_path.as_posix()}"
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
