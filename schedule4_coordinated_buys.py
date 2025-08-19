#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
schedule4_coordinated_buys.py  (aligned with FI version)

Flags “coordinated buys” in US CSV and writes flags back into the same file by default.

Usage:
  python schedule4_coordinated_buys.py --in out.csv [--out out_with_coordinated.csv]
    [--by publication|transaction] [--abs_tol 0.02] [--pct_tol 0.003] [--min_buyers 2]
"""

import argparse
import numpy as np
import pandas as pd

ACQ_CODES = {"P", "A", "C"}  # acquisition-like Schedule 4 codes

def to_float(x):
    try:
        return float(x)
    except Exception:
        return np.nan

def compute_flags(df: pd.DataFrame, by: str, abs_tol: float, pct_tol: float, min_buyers: int) -> pd.DataFrame:
    df = df.copy()
    # normalize dates
    df["trade_date"] = pd.to_datetime(df.get("trade_date"), errors="coerce").dt.date
    df["filing_date"] = pd.to_datetime(df.get("filing_date"), errors="coerce").dt.date
    # normalize buyer id
    df["_buyer_norm"] = df.get("buyer", "").astype(str).str.strip().str.upper()
    # normalize price
    df["price"] = pd.to_numeric(df.get("price"), errors="coerce").map(to_float)
    # acquisition code filter
    tcode = df.get("transaction_code")
    if tcode is not None:
        df["_is_buy"] = tcode.astype(str).str.strip().str.upper().isin(ACQ_CODES)
    else:
        df["_is_buy"] = True

    # choose date basis
    date_col = "filing_date" if by == "publication" else "trade_date"
    df["_group_date"] = df[date_col]

    # init outputs
    df["coordinated"] = False
    df["coordinated_buyers"] = 0
    df["coord_span_abs"] = np.nan
    df["coord_span_pct"] = np.nan
    df["coord_basis"] = f"issuer-{ 'filing' if by=='publication' else 'trade' }-date"

    # use only buys for grouping
    df_buy = df[df["_is_buy"]].copy()
    if df_buy.empty:
        df.drop(columns=["_buyer_norm","_group_date","_is_buy"], inplace=True, errors="ignore")
        return df

    # group by issuer + chosen date; add ticker to avoid cross-issuer/ticker mingling if desired (kept simple: issuer-date)
    group_keys = ["issuer", "_group_date"]

    for keys, sub in df_buy.groupby(group_keys, dropna=False):
        buyers = sub["_buyer_norm"].dropna().unique()
        if len(buyers) < min_buyers:
            continue
        prices = sub["price"].dropna().values
        if prices.size < min_buyers:
            continue
        pmin, pmax = float(np.min(prices)), float(np.max(prices))
        pmed = float(np.median(prices)) if prices.size else np.nan
        abs_ok = (pmax - pmin) <= abs_tol
        pct_ok = False
        if not np.isnan(pmed) and pmed != 0:
            pct_ok = (pmax - pmin) <= (pct_tol * abs(pmed))
        if not (abs_ok or pct_ok):
            continue

        mask = (df["issuer"] == sub["issuer"].iloc[0]) & (df["_group_date"] == sub["_group_date"].iloc[0])
        df.loc[mask, "coordinated"] = True
        df.loc[mask, "coordinated_buyers"] = int(len(buyers))
        df.loc[mask, "coord_span_abs"] = pmax - pmin
        df.loc[mask, "coord_span_pct"] = (pmax - pmin) / (abs(pmed) if pmed not in (0, np.nan) else 1.0)
        df.loc[mask, "coord_basis"] = f"issuer-{ 'filing' if by=='publication' else 'trade' }-date"

    df.drop(columns=["_buyer_norm","_group_date","_is_buy"], inplace=True, errors="ignore")
    return df

def main():
    ap = argparse.ArgumentParser(description="Flag coordinated buys (US) and write columns back in-place by default.")
    ap.add_argument("--in", dest="in_path", default="out.csv", help="Input CSV (default: out.csv)")
    ap.add_argument("--out", dest="out_path", default=None, help="If omitted, updates --in in-place.")
    ap.add_argument("--by", choices=["publication","transaction"], default="publication")
    ap.add_argument("--abs_tol", type=float, default=0.02)
    ap.add_argument("--pct_tol", type=float, default=0.003)
    ap.add_argument("--min_buyers", type=int, default=2)
    args = ap.parse_args()

    in_path = args.in_path
    out_path = args.out_path or in_path

    df = pd.read_csv(in_path)
    if df.empty:
        # ensure consistent columns
        for c, default in [
            ("coordinated", False),
            ("coordinated_buyers", 0),
            ("coord_span_abs", np.nan),
            ("coord_span_pct", np.nan),
            ("coord_basis", ""),
        ]:
            if c not in df.columns:
                df[c] = default
        df.to_csv(out_path, index=False)
        print(f"No rows. -> {out_path}")
        return

    flagged = compute_flags(df.copy(), by=args.by, abs_tol=args.abs_tol, pct_tol=args.pct_tol, min_buyers=args.min_buyers)

    # overlay only the coordinated columns on the original frame
    out_cols = ["coordinated","coordinated_buyers","coord_span_abs","coord_span_pct","coord_basis"]
    for c, default in [
        ("coordinated", False),
        ("coordinated_buyers", 0),
        ("coord_span_abs", np.nan),
        ("coord_span_pct", np.nan),
        ("coord_basis", ""),
    ]:
        if c not in df.columns:
            df[c] = default
        df[c] = flagged[c].values

    df.to_csv(out_path, index=False)
    print(f"Processed {len(df)} rows. Coordinated trades: {int(df['coordinated'].sum())}. -> {out_path}")

if __name__ == "__main__":
    main()
