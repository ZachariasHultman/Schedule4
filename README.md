# Schedule4 – Corporate & Insider Buys Tracker

Python tooling for parsing **SEC Schedule 4** filings from EDGAR, filtering for **corporate acquisitions** (≥10% owner) and detecting **coordinated buys**.

## Features
- Downloads and parses SEC EDGAR daily index files.
- Filters Schedule 4 filings where the **reporting person** is a corporation or large shareholder.
- Extracts:
  - Buyer name
  - Issuer (company bought)
  - Ticker
  - Trade date
  - Filing date
  - Transaction code
  - Price
- Detects **coordinated buys**:
  - Multiple distinct buyers in the same filing **or**
  - Multiple buyers for the same issuer/date across filings
  - Optional price tolerance filter (default ±$0.02)
- Outputs to CSV

## Example Usage
```bash
# Pull Schedule 4 data for a date range
python schedule4_corporate_buys.py \
    --start 2025-08-11 \
    --end 2025-08-12 \
    --csv out.csv \
    --user_agent "Your Name <your@email.com>" \
    --sleep 0.4 \
    --print_passed

# Flag coordinated buys in the output CSV
python schedule4_coordinated_buys.py