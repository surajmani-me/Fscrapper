# Fscrapper

Product-wise Flipkart reviews scraper with:

- URL normalization from product or product-reviews links
- Automatic cleanup of tracking query params
- Retry + exponential backoff
- Incremental dedupe using stable review hash
- One output file set per product
- Resume mode for continuation jobs
- Fail-soft processing across multiple products

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