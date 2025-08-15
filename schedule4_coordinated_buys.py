#!/usr/bin/env python3
import pandas as pd

# --- config ---
IN_PATH = "out.csv"
OUT_PATH = "out_with_coordinated.csv"
ACQ_CODES = {"P", "A", "C"}  # acquisition-like Schedule 4 codes
PRICE_TOL = 0.02  # USD tolerance within a group

# --- load ---
df = pd.read_csv(IN_PATH)

# normalize types
df["trade_date"] = pd.to_datetime(df["trade_date"], errors="coerce").dt.date
df["filing_date"] = pd.to_datetime(df["filing_date"], errors="coerce").dt.date
df["buyer_norm"] = df["buyer"].astype(str).str.strip().str.upper()
df["price"] = pd.to_numeric(df["price"], errors="coerce")

# base output columns
df["coordinated"] = False
df["coordinated_buyers"] = 0
df["coord_basis"] = ""  # '', 'filing', 'issuer', 'both'


def group_coord_stats(sub: pd.DataFrame) -> tuple[bool, int]:
    """
    Returns (is_coordinated, buyer_count_within_tol).
    Group is coordinated if:
      - >= 2 distinct buyers, AND
      - all non-null prices lie within PRICE_TOL range.
    """
    buyers = sub["buyer_norm"].dropna().unique()
    if len(buyers) < 2:
        return (False, 0)
    prices = sub["price"].dropna().unique()
    if len(prices) == 0:
        # no price info -> cannot assert price alignment
        return (False, 0)
    if prices.max() - prices.min() <= PRICE_TOL:
        return (True, len(buyers))
    return (False, 0)


# only consider acquisition-like codes
mask_acq = df["transaction_code"].isin(ACQ_CODES)

# Heuristic 1: same filing (accession_url) + trade_date + code
filing_key = ["accession_url", "trade_date", "transaction_code"]
filing_flags = pd.Series(False, index=df.index)
filing_counts = pd.Series(0, index=df.index)

for keys, sub in df[mask_acq].groupby(filing_key):
    is_coord, nbuyers = group_coord_stats(sub)
    if is_coord:
        filing_flags.loc[sub.index] = True
        filing_counts.loc[sub.index] = nbuyers

# Heuristic 2: same issuer/ticker + trade_date + code
issuer_key = ["issuer", "ticker", "trade_date", "transaction_code"]
issuer_flags = pd.Series(False, index=df.index)
issuer_counts = pd.Series(0, index=df.index)

for keys, sub in df[mask_acq].groupby(issuer_key):
    is_coord, nbuyers = group_coord_stats(sub)
    if is_coord:
        issuer_flags.loc[sub.index] = True
        issuer_counts.loc[sub.index] = nbuyers

# Combine
df["coordinated"] = filing_flags | issuer_flags
df["coordinated_buyers"] = (
    pd.concat([filing_counts, issuer_counts], axis=1).max(axis=1).astype(int)
)


def basis_row(f_flag: bool, i_flag: bool) -> str:
    if f_flag and i_flag:
        return "both"
    if f_flag:
        return "filing"
    if i_flag:
        return "issuer"
    return ""


df["coord_basis"] = [basis_row(f, i) for f, i in zip(filing_flags, issuer_flags)]

# cleanup helper column
df.drop(columns=["buyer_norm"], inplace=True)

# save
df.to_csv(OUT_PATH, index=False)
print(f"Processed {len(df)} rows.")
print(f"Coordinated trades: {int(df['coordinated'].sum())}")
print(f"Saved to {OUT_PATH}")
