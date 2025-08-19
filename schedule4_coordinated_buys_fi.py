#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
schedule4_coordinated_buys_fi.py  (dedupe-aware)

Flags “coordinated buys” in FI CSV while removing duplicate rows caused by
'Revised' / 'History' publications.

Usage:
  python schedule4_coordinated_buys_fi.py --in out_fi.csv --out out_fi_flagged.csv
    [--by publication|transaction] [--abs_tol 0.02] [--pct_tol 0.003] [--min_buyers 2]
    [--keep_history] [--no_prefer_revised]
"""

import argparse, math, re
from typing import Tuple
import numpy as np
import pandas as pd

BUY_PAT = re.compile(r"(acquisition|purchase|förvärv|köp)", re.IGNORECASE)

COLMAP = {
    "publication date": "pub_date",
    "publiceringsdatum": "pub_date",
    "transaction date": "tx_date",
    "transaktionsdatum": "tx_date",
    "issuer": "issuer",
    "emittent": "issuer",
    "person discharging managerial responsibilities": "buyer",
    "person i ledande ställning": "buyer",
    "closely associated": "associated",
    "närstående": "associated",
    "nature of transaction": "nature",
    "karaktär": "nature",
    "transaktionstyp": "nature",
    "instrument name": "instrument",
    "instrumentnamn": "instrument",
    "instrument type": "instrument_type",
    "instrumenttyp": "instrument_type",
    "isin": "isin",
    "volume": "volume",
    "volym": "volume",
    "unit": "unit",
    "volymsenhet": "unit",
    "price": "price",
    "pris": "price",
    "currency": "currency",
    "valuta": "currency",
    "status": "status",
    "details": "details",
}


def norm(s):
    return re.sub(r"\s+", " ", str(s)).strip() if s is not None else ""


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    rename = {}
    for c in df.columns:
        key = norm(c).lower().replace("–", "-").replace("—", "-")
        key = re.sub(r"\s+", " ", key)
        rename[c] = COLMAP.get(key, key)
    return df.rename(columns=rename)


def to_float(x):
    if x is None or (isinstance(x, float) and math.isnan(x)):
        return np.nan
    s = str(x).replace("\u00a0", " ").strip().replace(" ", "")
    if s.count(",") > 0 and s.count(".") == 0:
        s = s.replace(",", ".")
    m = re.search(r"-?\d+(?:[.,]\d+)?", s)
    if not m:
        return np.nan
    try:
        return float(m.group(0).replace(",", "."))
    except:
        return np.nan


def parse_date_series(s: pd.Series) -> pd.Series:
    # FI HTML/CSV: DD/MM/YYYY; also accept ISO
    return pd.to_datetime(s, errors="coerce", dayfirst=True).dt.date


def choose_date_column(df: pd.DataFrame, by: str) -> Tuple[str, pd.Series]:
    if by == "publication":
        if "pub_date" not in df.columns:
            raise SystemExit("Missing Publication date/Publiceringsdatum.")
        return "pub_date", parse_date_series(df["pub_date"])
    else:
        if "tx_date" not in df.columns:
            raise SystemExit("Missing Transaction date/Transaktionsdatum.")
        return "tx_date", parse_date_series(df["tx_date"])


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="in_path", required=True)
    ap.add_argument("--out", dest="out_path", default=None,
                    help="If omitted, updates --in in-place.")
    ap.add_argument(
        "--by", choices=["publication", "transaction"], default="publication"
    )
    ap.add_argument("--abs_tol", type=float, default=0.02)
    ap.add_argument("--pct_tol", type=float, default=0.003)
    ap.add_argument("--min_buyers", type=int, default=2)
    ap.add_argument(
        "--keep_history",
        action="store_true",
        help="Keep rows with Status='History' (default: drop)",
    )
    ap.add_argument(
        "--no_prefer_revised",
        action="store_true",
        help="Do not prefer 'Revised' over 'Notification' on duplicate keys",
    )
    args = ap.parse_args()

    out_path = args.out_path or args.in_path
    df = pd.read_csv(args.in_path)
    if df.empty:
        df.to_csv(out_path, index=False)
        print("No rows.")
        return

    df = normalize_columns(df)
    need = {"issuer", "nature"}
    missing = need - set(df.columns)
    if missing:
        raise SystemExit(f"Missing columns after normalization: {missing}")

    # Date basis
    date_col_name, group_date = choose_date_column(df, args.by)
    df["_group_date"] = group_date

    # Normalize buyer id (include associated if present)
    if "buyer" not in df.columns:
        df["buyer"] = ""
    assoc = df["associated"].fillna("") if "associated" in df.columns else ""
    df["_buyer_norm"] = df["buyer"].fillna("").astype(str).str.strip().str.upper() + (
        " / " + assoc.astype(str).str.strip().str.upper()
    ).where(assoc.astype(str).str.len() > 0, "")

    # Price/Volume/Currency
    if "price" not in df.columns:
        df["price"] = np.nan
    df["price"] = df["price"].map(to_float)
    if "volume" in df.columns:
        df["volume"] = df["volume"].map(to_float)
    cur = df["currency"] if "currency" in df.columns else pd.Series("", index=df.index)
    isin = df["isin"] if "isin" in df.columns else pd.Series("", index=df.index)
    instr = (
        df["instrument"]
        if "instrument" in df.columns
        else pd.Series("", index=df.index)
    )

    # Keep only buys
    df["_is_buy"] = df["nature"].astype(str).str.contains(BUY_PAT)
    df = df[df["_is_buy"]].copy()

    # ---- DEDUPE PHASE ----
    # 1) Drop History (unless kept)
    if not args.keep_history and "status" in df.columns:
        df = df[
            ~df["status"].astype(str).str.contains(r"^history$", case=False, na=False)
        ]

    # 2) Prefer Revised over Notification on duplicate keys
    # Rank: Revised(2) > Notification(1) > other/NaN(0)
    if not args.no_prefer_revised and "status" in df.columns:
        status = df["status"].astype(str).str.strip().str.lower()
        rank = np.where(
            status.eq("revised"), 2, np.where(status.eq("notification"), 1, 0)
        )
        df["_status_rank"] = rank
    else:
        df["_status_rank"] = 0

    # Deduplication key (avoid counting the same trade twice)
    # Keys: issuer, group_date, tx_date, isin, currency, instrument, volume, price, buyer_norm
    if "tx_date" not in df.columns:
        df["tx_date"] = ""
    dkey_cols = ["issuer", "_group_date", "tx_date", "_buyer_norm", "price", "volume"]
    if "currency" in df.columns:
        dkey_cols.append("currency")
    if "isin" in df.columns:
        dkey_cols.append("isin")
    if "instrument" in df.columns:
        dkey_cols.append("instrument")

    # For stable dedupe: sort by rank desc, then drop duplicates keeping first (best)
    df = df.sort_values(["_status_rank"], ascending=False)
    df = df.drop_duplicates(subset=dkey_cols, keep="first").copy()

    # ---- COORDINATION PHASE ----
    df["coordinated"] = False
    df["coordinated_buyers"] = 0
    df["coord_span_abs"] = np.nan
    df["coord_span_pct"] = np.nan
    df["coord_basis"] = "issuer-date"

    # Group key: issuer + chosen date (+ currency + ISIN to avoid mixing)
    group_keys = ["issuer", "_group_date"]
    if "currency" in df.columns:
        group_keys.append("currency")
    if "isin" in df.columns:
        group_keys.append("isin")

    for keys, sub in df.groupby(group_keys, dropna=False):
        buyers = sub["_buyer_norm"].dropna().unique()
        if len(buyers) < args.min_buyers:
            continue

        prices = sub["price"].dropna().values
        if prices.size < args.min_buyers:
            continue

        pmin, pmax = float(np.min(prices)), float(np.max(prices))
        pmed = float(np.median(prices))
        abs_ok = (pmax - pmin) <= args.abs_tol
        pct_ok = (pmax - pmin) <= (args.pct_tol * max(pmed, 1e-9))
        if not (abs_ok or pct_ok):
            continue

        mask = (df["issuer"] == sub["issuer"].iloc[0]) & (
            df["_group_date"] == sub["_group_date"].iloc[0]
        )
        if "currency" in df.columns:
            mask &= cur == (
                sub["currency"].iloc[0] if "currency" in sub.columns else ""
            )
        if "isin" in df.columns:
            mask &= isin == (sub["isin"].iloc[0] if "isin" in sub.columns else "")

        df.loc[mask, "coordinated"] = True
        df.loc[mask, "coordinated_buyers"] = int(len(buyers))
        df.loc[mask, "coord_span_abs"] = pmax - pmin
        df.loc[mask, "coord_span_pct"] = (pmax - pmin) / max(pmed, 1e-9)

    # Cleanup & save
    df.drop(
        columns=["_is_buy", "_buyer_norm", "_group_date", "_status_rank"],
        inplace=True,
        errors="ignore",
    )
    df.to_csv(out_path, index=False)
    print(f"Done. Coordinated rows: {int(df['coordinated'].sum())}. -> {out_path}")


if __name__ == "__main__":
    main()
