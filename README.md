# Schedule 4 Insider Transactions

Pipeline to scrape insider transactions (US SEC Form 4 + Swedish FI PDMR) and flag **coordinated buys/sales**.

---

## Transaction Codes & Direction Mapping

### SEC Form 4 (US)
| Code | Meaning                                    | Direction |
|------|--------------------------------------------|-----------|
| **P** | Open‑market purchase                       | Buy |
| **A** | Grant/award/other acquisition              | Buy |
| **C** | Conversion of derivative                   | Buy |
| **S** | Open‑market sale                           | Sell |
| **D** | Sale/disposition (non‑open market)         | Sell |
| **F** | Tax payment with securities                | Sell |

> Override with `--include_codes` on the US scraper if you want more/less.

### FI “Nature of transaction” (regex keywords)
- **Buy:** `acquisition|purchase|förvärv|köp`
- **Sell:** `sale|disposal|försälj|avyttr`

Regex is case‑insensitive and applied to the normalized “Nature/Karaktär/Transaktionstyp” text.

---

## Quick Start

```bash
# 1) Scrape latest available (non‑holiday) day from both sources
python run_scrapers.py \
  --us_csv out.csv \
  --fi_out out_fi.csv \
  --us_days 1 --fi_days 1 \
  --include_codes "P,C,A,S,D,F" \
  --user_agent "Your Name email@domain"

# 2) Flag coordinated buys AND sales (in‑place by default)
python run_coordinated_flagging.py \
  --us_in out.csv \
  --fi_in out_fi.csv \
  --direction both \
  --by publication \
  --abs_tol 0.02 \
  --pct_tol 0.003 \
  --min_parties 2
```

Outputs are the same CSVs with extra columns (see **Flag Output Columns**).

---

## Scripts

### `run_scrapers.py` — scrape US + FI
Runs both scrapers without changing their logic.

**Usage**
```bash
python run_scrapers.py \
  --us_csv out.csv \
  --fi_out out_fi.csv \
  --us_days 1 \
  --fi_days 1 \
  --include_codes "P,C,A,S,D,F" \
  [--user_agent "Your Name email@domain"] \
  [--print_passed] [--keep_otc] [--no_tenpct_filter] \
  [--sleep 0.3] \
  [--fi_issuer "Intrum"]
```

**Options**
- `--us_csv` (default: `out.csv`): output path for the US scraper.
- `--fi_out` (default: `out_fi.csv`): output path for the FI scraper.
- `--us_days` / `--fi_days` (default: `1`): how many **latest available publication days** to fetch (handles weekends/holidays).
- `--include_codes` (US): Form 4 codes to include; default covers **buys + sales** (`P,C,A,S,D,F`).
- `--user_agent` (US): SEC requires a UA string containing contact info.
- `--print_passed` (US): log excluded transactions.
- `--keep_otc` (US): keep OTC tickers (default is to drop).
- `--no_tenpct_filter` (US): include 10% owners (default is to filter out).
- `--sleep` (US/FI): throttle requests.
- `--fi_issuer` (FI): restrict by issuer name (optional).

**Outputs**
- `out.csv` — US transactions (filtered by `--include_codes`).
- `out_fi.csv` — FI transactions (latest publication day(s)).

---

### `run_coordinated_flagging.py` — add coordinated flags
Runs both coordinated‑flaggers and writes flags **in‑place** by default.

**Usage**
```bash
python run_coordinated_flagging.py \
  --us_in out.csv \
  --fi_in out_fi.csv \
  [--us_out out_with_flags.csv] \
  [--fi_out out_fi_flagged.csv] \
  --direction both|buy|sell \
  --by publication|transaction \
  --abs_tol 0.02 \
  --pct_tol 0.003 \
  --min_parties 2 \
  [--keep_history] \
  [--no_prefer_revised]
```

**Common options (US + FI)**
- `--direction` (default `both`): flag buys, sells, or both.
- `--by` (default `publication`): `publication` = filing/publication date; `transaction` = trade date.
- `--abs_tol` (default `0.02`): absolute price spread allowed within a group.
- `--pct_tol` (default `0.003`): percent spread allowed (relative to median price).
- `--min_parties` (default `2`): minimum distinct insiders in the group.

**FI‑specific computation toggles**
- `--keep_history`: keep `Status=History` rows in *computation* (default drops them).
- `--no_prefer_revised`: do **not** prefer `Revised` over `Notification` for duplicates (default prefers Revised).

> If `--out` is omitted, the input CSV is updated in‑place.

---

## What the Scrapers Keep (US)
The US scraper reports `Done. Total kept: X (from Y parsed transactions).`  
“Kept” means rows that pass the configured filters:
- Form 4 transaction code ∈ `--include_codes` (default: P,C,A,S,D,F).
- Drops OTC unless `--keep_otc`.
- Drops 10% owners unless `--no_tenpct_filter`.

Only “kept” rows are written to `out.csv` for downstream flagging.

---

## Coordination Logic

For each **issuer + date** group (date depends on `--by`):
1. Filter to **direction** (buy/sell) subset.
2. (FI only) Dedupe duplicates; prefer `Revised` over `Notification` unless toggled.
3. Compute price span `[min, max]` and median.
4. Flag group if **either**:  
   - `(pmax - pmin) ≤ abs_tol`, **or**  
   - `(pmax - pmin) ≤ pct_tol × median(price)`  
   and there are at least `min_parties` distinct insiders.
5. Broadcast flags back to all matching rows for that issuer/date (and ISIN/currency for FI).

---

## Flag Output Columns

### Buys
- `coordinated_buy` — bool
- `coordinated_buy_parties` — int
- `coord_buy_span_abs` — float
- `coord_buy_span_pct` — float
- `coord_buy_basis` — str

**Back‑compat (buy alias):**
- `coordinated` (alias of `coordinated_buy`)
- `coordinated_buyers` (alias of `coordinated_buy_parties`)
- `coord_span_abs` / `coord_span_pct` / `coord_basis` (aliases of buy columns)

### Sells
- `coordinated_sell` — bool
- `coordinated_sell_parties` — int (FI uses `coordinated_sellers` internally, but merged to this name where aligned)
- `coord_sell_span_abs` — float
- `coord_sell_span_pct` — float
- `coord_sell_basis` — str

> Non‑matching rows (e.g., sells when `--direction buy`) receive defaults: `False/0/NaN/""`.

---

## Examples

### Buys only for last 3 days; write to new files
```bash
python run_scrapers.py --us_csv us.csv --fi_out fi.csv --us_days 3 --fi_days 3 --include_codes "P,C,A"
python run_coordinated_flagging.py --us_in us.csv --fi_in fi.csv --direction buy --us_out us_buy_flags.csv --fi_out fi_buy_flags.csv
```

### FI only for a single issuer (Intrum); sales only
```bash
python run_scrapers.py --fi_out fi_intrum.csv --fi_days 2 --fi_issuer "Intrum"
python run_coordinated_flagging.py --fi_in fi_intrum.csv --direction sell
```

### Keep OTC and 10% owners in US scraping
```bash
python run_scrapers.py --us_csv us_all.csv --include_codes "P,C,A,S,D,F" --keep_otc --no_tenpct_filter
python run_coordinated_flagging.py --us_in us_all.csv --direction both
```

---

## Operational Notes
- **Latest non‑holiday day** is used by default (`--us_days 1`, `--fi_days 1`); weekends/holidays auto‑handled.
- Provide a proper **SEC User‑Agent** string: `"Your Name email@domain"`.
- All coordinations are price‑tolerance‑based; adjust `--abs_tol` and `--pct_tol` to tighten/loosen.
