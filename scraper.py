
#!/usr/bin/env python3
"""Product-wise Flipkart reviews scraper.

This tool is designed for compliant data collection workflows where you own the
input product list and operate within website terms and local laws.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import random
import re
import time
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

import requests
from bs4 import BeautifulSoup


DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-IN,en;q=0.9",
}

TRACKING_QUERY_PREFIXES = (
    "otracker",
    "as",
)

TRACKING_QUERY_KEYS = {
    "iid",
    "ppt",
    "ppn",
    "ssid",
    "qH",
    "requestId",
    "suggestionId",
    "fm",
}


@dataclass
class ReviewRecord:
    product_id: str
    product_url: str
    review_page: int
    review_id_hash: str
    rating: int | None
    title: str
    review_text: str
    reviewer: str
    review_date: str
    variant: str
    helpful_count: int | None
    scraped_at: str


def normalize_space(value: str) -> str:
    return re.sub(r"\s+", " ", (value or "").strip())


def extract_urls_from_text(text: str) -> list[str]:
    return re.findall(r"https?://[^\s\]\[\"'<>]+", text)


def canonicalize_url(url: str) -> str:
    parsed = urlparse(url.strip())
    if not parsed.scheme:
        parsed = urlparse(f"https://{url.strip()}")

    query = parse_qs(parsed.query)
    cleaned_query: dict[str, list[str]] = {}
    for key, values in query.items():
        low = key.lower()
        if low in TRACKING_QUERY_KEYS:
            continue
        if any(low.startswith(prefix) for prefix in TRACKING_QUERY_PREFIXES):
            continue
        cleaned_query[key] = values

    query_str = urlencode({k: v[0] for k, v in cleaned_query.items()}, doseq=False)
    return urlunparse((parsed.scheme or "https", parsed.netloc, parsed.path, "", query_str, ""))


def to_int(value: Any) -> int | None:
    if value is None:
        return None
    raw = normalize_space(str(value))
    if not raw:
        return None
    m = re.search(r"\d+", raw.replace(",", ""))
    if not m:
        return None
    try:
        return int(m.group(0))
    except ValueError:
        return None


def extract_pid(url: str) -> str:
    parsed = urlparse(url)
    query = parse_qs(parsed.query)
    pid_values = query.get("pid")
    if pid_values:
        return pid_values[0]

    m = re.search(r"\b([A-Z0-9]{10,20})\b", parsed.path)
    if m:
        return m.group(1)
    return "UNKNOWN"


def to_review_url(product_url: str, page: int) -> str:
    parsed = urlparse(canonicalize_url(product_url))
    query = parse_qs(parsed.query)
    pid = extract_pid(product_url)
    if pid != "UNKNOWN":
        query["pid"] = [pid]

    path = parsed.path
    if "/product-reviews/" not in path and "/p/" in path:
        path = path.replace("/p/", "/product-reviews/", 1)

    query["page"] = [str(page)]
    keep_keys = {"pid", "lid", "marketplace", "page"}
    query = {k: v for k, v in query.items() if k in keep_keys}
    query_str = urlencode({k: v[0] for k, v in query.items()}, doseq=False)

    return urlunparse((parsed.scheme or "https", parsed.netloc, path, "", query_str, ""))


def stable_hash(*parts: str) -> str:
    payload = "||".join(normalize_space(p) for p in parts)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def request_with_retry(
    session: requests.Session,
    url: str,
    timeout: int,
    max_retries: int,
    backoff_base: float,
) -> str:
    for attempt in range(1, max_retries + 1):
        try:
            response = session.get(url, timeout=timeout)
            if response.status_code in {429, 503}:
                raise requests.HTTPError(f"HTTP {response.status_code}", response=response)
            response.raise_for_status()
            return response.text
        except requests.RequestException:
            if attempt == max_retries:
                raise
            sleep_for = backoff_base * (2 ** (attempt - 1)) + random.uniform(0.2, 0.9)
            time.sleep(sleep_for)
    raise RuntimeError("Unreachable retry state")


def _json_walk(node: Any) -> Iterable[dict[str, Any]]:
    if isinstance(node, dict):
        yield node
        for value in node.values():
            yield from _json_walk(value)
    elif isinstance(node, list):
        for item in node:
            yield from _json_walk(item)


def parse_reviews_from_jsonld(soup: BeautifulSoup) -> list[dict[str, Any]]:
    reviews: list[dict[str, Any]] = []
    scripts = soup.find_all("script", attrs={"type": "application/ld+json"})
    for script in scripts:
        raw = script.string or script.get_text() or ""
        raw = raw.strip()
        if not raw:
            continue
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            continue
        for node in _json_walk(data):
            node_type = node.get("@type")
            if node_type == "Review" or (isinstance(node_type, list) and "Review" in node_type):
                author = node.get("author", {})
                reviewer = ""
                if isinstance(author, dict):
                    reviewer = normalize_space(str(author.get("name", "")))
                else:
                    reviewer = normalize_space(str(author))

                rr = node.get("reviewRating", {})
                rating = None
                if isinstance(rr, dict):
                    rating = to_int(rr.get("ratingValue"))

                helpful_count = None
                interaction = node.get("interactionStatistic")
                if isinstance(interaction, dict):
                    helpful_count = to_int(interaction.get("userInteractionCount"))

                reviews.append(
                    {
                        "rating": rating,
                        "title": normalize_space(str(node.get("name", ""))),
                        "review_text": normalize_space(str(node.get("reviewBody", ""))),
                        "reviewer": reviewer,
                        "review_date": normalize_space(str(node.get("datePublished", ""))),
                        "variant": normalize_space(str(node.get("sku", ""))),
                        "helpful_count": helpful_count,
                    }
                )
    return reviews


def first_text(block: Any, selectors: list[str]) -> str:
    for selector in selectors:
        node = block.select_one(selector)
        if node:
            value = normalize_space(node.get_text(" "))
            if value:
                return value
    return ""


def parse_reviews_from_html_fallback(soup: BeautifulSoup) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    blocks = soup.select("div.col.EPCmJX, div._1AtVbE, div._27M-vq, div._16PBlm")
    seen_local: set[str] = set()

    for block in blocks:
        text_blob = normalize_space(block.get_text(" "))
        if len(text_blob) < 60:
            continue
        if "buyer" not in text_blob.lower() and "read more" not in text_blob.lower() and "rating" not in text_blob.lower():
            continue

        rating = to_int(
            first_text(
                block,
                [
                    "div.XQDdHH",
                    "div._3LWZlK",
                    "div._3LWZlK._1BLPMq",
                    "span.XQDdHH",
                ],
            )
        )
        if rating is None:
            rating_match = re.search(r"\b([1-5])(\.[0-9])?\s*(stars?)?\b", text_blob, flags=re.IGNORECASE)
            if rating_match:
                rating = to_int(rating_match.group(1))

        title = first_text(block, ["p.z9E0IG", "p._2-N8zT", "p._2xg6Ul", "div._6K-7Co"])
        review_text = first_text(block, ["div.ZmyHeo", "div.t-ZTKy", "div._11pzQk", "div._6K-7Co"])
        if not review_text:
            review_text = text_blob

        reviewer = first_text(block, ["p._2sc7ZR", "span._2sc7ZR", "p._2V5EHH"])
        review_date = first_text(block, ["p._2mcZGG", "p._2NsDsF", "span._2sc7ZR+span"])
        if not review_date:
            date_match = re.search(r"\b\d{1,2}\s+[A-Za-z]+\s*,?\s*\d{4}\b", text_blob)
            if date_match:
                review_date = normalize_space(date_match.group(0))

        if not reviewer:
            reviewer_date_match = re.search(
                r"([A-Za-z0-9_.\- ]+)\s*,\s*(\d{1,2}\s+[A-Za-z]+\s*\d{4})",
                text_blob,
            )
            if reviewer_date_match:
                reviewer = normalize_space(reviewer_date_match.group(1))
                if not review_date:
                    review_date = normalize_space(reviewer_date_match.group(2))

        variant = ""
        variant_match = re.search(r"(?:Color|Storage|RAM|Variant)\s*:\s*([^|,]+)", text_blob, flags=re.IGNORECASE)
        if variant_match:
            variant = normalize_space(variant_match.group(1))

        helpful_match = re.search(r"(\d[\d,]*)\s+people\s+found\s+this\s+helpful", text_blob, flags=re.IGNORECASE)
        helpful_count = to_int(helpful_match.group(1)) if helpful_match else None

        local_hash = stable_hash(reviewer, review_date, title, review_text[:160])
        if local_hash in seen_local:
            continue
        seen_local.add(local_hash)

        records.append(
            {
                "rating": rating,
                "title": title,
                "review_text": review_text,
                "reviewer": reviewer,
                "review_date": review_date,
                "variant": variant,
                "helpful_count": helpful_count,
            }
        )
    return records


def parse_review_page(html: str) -> list[dict[str, Any]]:
    soup = BeautifulSoup(html, "html.parser")

    parsed = parse_reviews_from_jsonld(soup)

    if parsed:
        return parsed

    return parse_reviews_from_html_fallback(soup)


def load_urls_from_input(path: Path) -> list[str]:
    text = path.read_text(encoding="utf-8").strip()
    if not text:
        return []

    if path.suffix.lower() == ".json":
        blob = json.loads(text)
        if isinstance(blob, list):
            return [str(x).strip() for x in blob if str(x).strip()]
        raise ValueError("JSON input must be an array of URLs")

    if path.suffix.lower() == ".csv":
        rows: list[str] = []
        with path.open("r", encoding="utf-8", newline="") as fh:
            reader = csv.DictReader(fh)
            fieldnames = reader.fieldnames or []
            url_col = "url" if "url" in fieldnames else "product_url" if "product_url" in fieldnames else None
            if not url_col:
                raise ValueError("CSV input must have a 'url' or 'product_url' column")
            for row in reader:
                u = (row.get(url_col) or "").strip()
                if u:
                    rows.append(u)
        return rows

    extracted = extract_urls_from_text(text)
    if extracted:
        return extracted

    return [line.strip() for line in text.splitlines() if line.strip() and not line.strip().startswith("#")]


def read_existing_hashes(jsonl_path: Path) -> set[str]:
    seen: set[str] = set()
    if not jsonl_path.exists():
        return seen

    with jsonl_path.open("r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue
            rid = row.get("review_id_hash")
            if isinstance(rid, str) and rid:
                seen.add(rid)
    return seen


def write_outputs(records: list[ReviewRecord], jsonl_path: Path, csv_path: Path, fmt: str) -> None:
    if not records:
        return

    if fmt in {"jsonl", "both"}:
        with jsonl_path.open("a", encoding="utf-8") as fh:
            for record in records:
                fh.write(json.dumps(asdict(record), ensure_ascii=True) + "\n")

    if fmt in {"csv", "both"}:
        file_exists = csv_path.exists()
        with csv_path.open("a", encoding="utf-8", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=list(asdict(records[0]).keys()))
            if not file_exists:
                writer.writeheader()
            for record in records:
                writer.writerow(asdict(record))


def scrape_product(
    session: requests.Session,
    product_url: str,
    output_dir: Path,
    max_pages: int,
    delay_min: float,
    delay_max: float,
    timeout: int,
    max_retries: int,
    backoff_base: float,
    seen_stop_threshold: int,
    max_empty_pages: int,
    fmt: str,
    resume: bool,
) -> tuple[str, int]:
    normalized_url = canonicalize_url(product_url)
    product_id = extract_pid(normalized_url)
    safe_product_id = re.sub(r"[^A-Za-z0-9_-]+", "_", product_id)

    jsonl_path = output_dir / f"{safe_product_id}.reviews.jsonl"
    csv_path = output_dir / f"{safe_product_id}.reviews.csv"

    seen_hashes = read_existing_hashes(jsonl_path) if resume else set()
    consecutive_stale_pages = 0
    consecutive_empty_pages = 0
    total_written = 0

    for page in range(1, max_pages + 1):
        review_url = to_review_url(normalized_url, page)
        html = request_with_retry(
            session=session,
            url=review_url,
            timeout=timeout,
            max_retries=max_retries,
            backoff_base=backoff_base,
        )
        page_rows = parse_review_page(html)
        if not page_rows:
            consecutive_empty_pages += 1
            if consecutive_empty_pages >= max_empty_pages:
                break
            time.sleep(random.uniform(delay_min, delay_max))
            continue

        consecutive_empty_pages = 0

        now_utc = datetime.now(timezone.utc).isoformat()
        batch: list[ReviewRecord] = []
        new_count = 0

        for row in page_rows:
            review_hash = stable_hash(
                product_id,
                row.get("reviewer", ""),
                row.get("review_date", ""),
                row.get("title", ""),
                row.get("review_text", "")[:200],
            )
            if review_hash in seen_hashes:
                continue

            seen_hashes.add(review_hash)
            new_count += 1
            batch.append(
                ReviewRecord(
                    product_id=product_id,
                    product_url=normalized_url,
                    review_page=page,
                    review_id_hash=review_hash,
                    rating=row.get("rating"),
                    title=row.get("title", ""),
                    review_text=row.get("review_text", ""),
                    reviewer=row.get("reviewer", ""),
                    review_date=row.get("review_date", ""),
                    variant=row.get("variant", ""),
                    helpful_count=row.get("helpful_count"),
                    scraped_at=now_utc,
                )
            )

        write_outputs(batch, jsonl_path=jsonl_path, csv_path=csv_path, fmt=fmt)
        total_written += len(batch)

        if new_count == 0:
            consecutive_stale_pages += 1
        else:
            consecutive_stale_pages = 0

        if consecutive_stale_pages >= seen_stop_threshold:
            break

        time.sleep(random.uniform(delay_min, delay_max))

    return product_id, total_written


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Product-wise Flipkart reviews scraper")
    parser.add_argument("--input", help="Path to .txt/.csv/.json file containing product URLs")
    parser.add_argument(
        "--url",
        action="append",
        default=[],
        help="Single product URL (repeatable)",
    )
    parser.add_argument("--output-dir", default="data/reviews", help="Output directory")
    parser.add_argument("--max-pages", type=int, default=50, help="Max review pages per product")
    parser.add_argument("--delay-min", type=float, default=1.2, help="Minimum delay between page requests")
    parser.add_argument("--delay-max", type=float, default=2.4, help="Maximum delay between page requests")
    parser.add_argument("--timeout", type=int, default=20, help="HTTP timeout in seconds")
    parser.add_argument("--max-retries", type=int, default=4, help="Retries per request")
    parser.add_argument("--backoff-base", type=float, default=1.5, help="Base delay for exponential backoff")
    parser.add_argument(
        "--seen-stop-threshold",
        type=int,
        default=2,
        help="Stop after N consecutive pages with no new reviews",
    )
    parser.add_argument(
        "--max-empty-pages",
        type=int,
        default=1,
        help="Stop after N consecutive pages where no reviews are parsed",
    )
    parser.add_argument(
        "--format",
        choices=["csv", "jsonl", "both"],
        default="both",
        help="Output format",
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Load existing JSONL hashes and skip already-seen reviews",
    )
    parser.add_argument(
        "--fail-fast",
        action="store_true",
        help="Abort on first product failure",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    if args.delay_min <= 0 or args.delay_max <= 0:
        raise SystemExit("--delay-min and --delay-max must be greater than 0")
    if args.delay_min > args.delay_max:
        raise SystemExit("--delay-min cannot be greater than --delay-max")
    if args.max_pages <= 0:
        raise SystemExit("--max-pages must be greater than 0")
    if args.seen_stop_threshold <= 0:
        raise SystemExit("--seen-stop-threshold must be greater than 0")
    if args.max_empty_pages <= 0:
        raise SystemExit("--max-empty-pages must be greater than 0")

    urls = [u.strip() for u in args.url if u and u.strip()]
    if args.input:
        urls.extend(load_urls_from_input(Path(args.input)))

    deduped_urls: list[str] = []
    seen_urls: set[str] = set()
    for u in urls:
        normalized = canonicalize_url(u)
        if normalized not in seen_urls:
            deduped_urls.append(normalized)
            seen_urls.add(normalized)

    if not deduped_urls:
        raise SystemExit("No URLs provided. Use --url and/or --input.")

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    session = requests.Session()
    session.headers.update(DEFAULT_HEADERS)

    grand_total = 0
    failures = 0
    for idx, product_url in enumerate(deduped_urls, start=1):
        try:
            product_id, written = scrape_product(
                session=session,
                product_url=product_url,
                output_dir=output_dir,
                max_pages=args.max_pages,
                delay_min=args.delay_min,
                delay_max=args.delay_max,
                timeout=args.timeout,
                max_retries=args.max_retries,
                backoff_base=args.backoff_base,
                seen_stop_threshold=args.seen_stop_threshold,
                max_empty_pages=args.max_empty_pages,
                fmt=args.format,
                resume=args.resume,
            )
            grand_total += written
            print(f"[{idx}/{len(deduped_urls)}] {product_id}: wrote {written} new reviews")
        except Exception as exc:  # pylint: disable=broad-except
            failures += 1
            print(f"[{idx}/{len(deduped_urls)}] ERROR for URL: {product_url}")
            print(f"  -> {type(exc).__name__}: {exc}")
            if args.fail_fast:
                raise

    print(f"Done. Total new reviews written: {grand_total}. Failed products: {failures}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
