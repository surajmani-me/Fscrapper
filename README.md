# Fscrapper

Product-wise Flipkart reviews scraper with:

- URL normalization from product or product-reviews links
- Automatic cleanup of tracking query params
- Retry + exponential backoff
- Incremental dedupe using stable review hash
- One output file set per product
- Resume mode for continuation jobs
- Fail-soft processing across multiple products

Also includes a sitemap-driven mobile catalog scraper that exports mobile and
smartphone product rows to CSV.

## Important

Use this only in ways allowed by Flipkart Terms, robots rules, and applicable law.

## Setup

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Quick Start

Single product URL:

```bash
python scraper.py \
	--url "https://www.flipkart.com/motorola-edge-70-fusion-pantone-country-air-128-gb/p/itmbae68fc092d22?pid=MOBHJ7TXKWJTAHNH" \
	--max-pages 20 \
	--resume
```

Multiple products via file:

```bash
python scraper.py --input products.txt --max-pages 50 --resume
```

Pasted raw text that includes URLs is also supported (the scraper auto-extracts URLs).

## Accepted Input File Formats

`products.txt` (one URL per line):

```text
https://www.flipkart.com/product-1/p/itmx?pid=MOBXXXX
https://www.flipkart.com/product-2/p/itmy?pid=MOBYYYY
```

`products.csv` (must contain `url` column):

```csv
url
https://www.flipkart.com/product-1/p/itmx?pid=MOBXXXX
https://www.flipkart.com/product-2/p/itmy?pid=MOBYYYY
```

`products.json` (array of URLs):

```json
[
	"https://www.flipkart.com/product-1/p/itmx?pid=MOBXXXX",
	"https://www.flipkart.com/product-2/p/itmy?pid=MOBYYYY"
]
```

## Output

Default folder: `data/reviews`

Per product, scraper writes:

- `<PRODUCT_ID>.reviews.jsonl`
- `<PRODUCT_ID>.reviews.csv`

Review schema:

- `product_id`
- `product_url`
- `review_page`
- `review_id_hash`
- `rating`
- `title`
- `review_text`
- `reviewer`
- `review_date`
- `variant`
- `helpful_count`
- `scraped_at`

## CLI Options

```bash
python scraper.py --help
```

Common options:

- `--url` (repeatable)
- `--input` (`.txt`, `.csv`, `.json`)
- `--max-pages` (default: `50`)
- `--delay-min` / `--delay-max` (default: `1.2` / `2.4` seconds)
- `--max-retries` (default: `4`)
- `--seen-stop-threshold` (default: `2` pages)
- `--max-empty-pages` (default: `1` page)
- `--format` (`csv`, `jsonl`, `both`)
- `--resume` (skip already-seen reviews)
- `--fail-fast` (abort immediately on first product error)

## Notes

- Parser first tries structured `application/ld+json` review data.
- If unavailable, it falls back to HTML extraction heuristics.
- Incremental runs are fastest with `--resume`.
- URL inputs are deduplicated after normalization.

## Mobile And Smartphone Catalog Scraper

Use `mobile_catalog_scraper.py` to discover product URLs from Flipkart product
sitemaps, validate product pages, and export mobile/smartphone rows to CSV.

If sitemap endpoints are blocked in your environment, it automatically falls
back to seed product URL graph crawling.

Quick run (small sample):

```bash
python mobile_catalog_scraper.py --max-products 100 --max-shards 10 --resume
```

Output defaults to:

- `data/catalog/mobile_smartphones.csv`

Use your curl/browser cookies (optional) if you face intermittent blocking:

```bash
python mobile_catalog_scraper.py \
	--cookie "<paste cookie header value>" \
	--extra-header "Referer: https://www.flipkart.com/mobile-phone-ab-at-store" \
	--max-products 500 --max-shards 40 --resume
```

Search-based discovery (matches your curl style):

```bash
python mobile_catalog_scraper.py \
	--discovery-mode search \
	--search-url "https://www.flipkart.com/search?q=mobiles&otracker=search&otracker1=search&marketplace=FLIPKART&as-show=off&as=off" \
	--search-pages 30 \
	--cookie "<paste cookie header value>" \
	--extra-header "Referer: https://www.flipkart.com/mobile-phone-ab-at-store" \
	--max-products 2000 --resume
```

Seed-based fallback crawl example:

```bash
python mobile_catalog_scraper.py \
	--seed-url "https://www.flipkart.com/motorola-g96-5g-pantone-dresden-blue-128-gb/p/itm3d5ad13991fdc" \
	--discovery-mode seed \
	--seed-max-depth 4 \
	--max-products 500 --max-candidates 2000 --resume
```

Useful flags:

- `--max-products` number of rows to write
- `--max-candidates` number of sitemap URLs to inspect
- `--max-shards` number of sitemap shard files to read (`0` for all)
- `--discovery-mode` `auto`, `search`, `sitemap`, or `seed`
- `--search-url` search/listing URL for candidate discovery
- `--search-pages` number of paginated search pages to read
- `--seed-url` fallback seed product URL (repeatable)
- `--seed-file` text file with seed URLs
- `--seed-max-depth` BFS depth for fallback graph crawl
- `--loose-url-filter` scan broader candidate URLs before classification
- `--include-accessories` include non-phone rows too
- `--resume` skip already-saved product IDs