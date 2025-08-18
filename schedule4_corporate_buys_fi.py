#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
FI PDMR – hämta ENDAST de 3 senaste publiceringsdagarna (eller valfritt N).
Fungerar även när “senaste dagarna” är helg/helgdagar – vi tar de 3 SENASTE
datum som faktiskt finns i listan på FI.

Exempel:
  python fi_last3.py --out out_fi_last3.csv
  python fi_last3.py --out out_fi_last3.csv --issuer "Intrum" --days 5
"""

import argparse, io, sys, time
from datetime import datetime, date
import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

BASE = "https://marknadssok.fi.se/publiceringsklient/en-GB/Search/Search"
UA = "InsynLast3/1.0 (+you@example.com)"
HEADERS = {"User-Agent": UA, "Accept-Language": "en-GB,en;q=0.8,sv-SE;q=0.7"}


def make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(HEADERS)
    retry = Retry(
        total=6,
        connect=6,
        read=6,
        backoff_factor=0.7,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=8, pool_maxsize=8)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    return s


def dparse(s: str):
    s = str(s).strip()
    for fmt in ("%d/%m/%Y", "%Y-%m-%d", "%d-%m-%Y"):
        try:
            return datetime.strptime(s, fmt).date()
        except:
            pass
    return None


def fetch_page(
    session: requests.Session, page: int, timeout=60, tries=4, sleep=0.8
) -> str:
    params = {
        "SearchFunctionType": "Insyn",
        "button": "search",
        "Page": page,
        "paging": "True",
    }
    for i in range(tries):
        try:
            r = session.get(BASE, params=params, timeout=timeout)
            r.raise_for_status()
            print(f"[DEBUG] GET p={page} -> {r.status_code} ({len(r.text)} bytes)")
            return r.text
        except requests.RequestException as e:
            wait = (2**i) * sleep
            print(f"[WARN] page {page} fetch error: {e} -> retry in {wait:.1f}s")
            time.sleep(wait)
    raise RuntimeError(f"Failed to fetch page {page} after {tries} tries")


def extract_table(html: str) -> pd.DataFrame | None:
    try:
        tbls = pd.read_html(io.StringIO(html))
    except ValueError:
        return None
    for t in tbls:
        cols = [str(c).strip().lower() for c in t.columns]
        if any(c.startswith("publication date") for c in cols):
            return t
    return None


def scrape_last_n_publication_days(
    days: int, issuer_sub: str | None, start_page: int, max_pages: int, sleep: float
) -> pd.DataFrame:
    """
    Gå nedåt i listan tills vi har hittat N unika publiceringsdatum.
    Fortsätt tills sidorna blir äldre än det äldsta av de N – då kan vi stoppa.
    """
    sess = make_session()
    collected_frames = []
    unique_dates: list[date] = []

    # Hjälp: normalisera kolumnnamn (eng)
    def normalize_cols(t: pd.DataFrame) -> pd.DataFrame:
        return t.rename(
            columns={
                "Publication date": "Publication date",
                "Issuer": "Issuer",
                "Person discharging managerial responsibilities": "PDMR",
                "Position": "Position",
                "Nature of transaction": "Nature",
                "Instrument name": "Instrument",
                "Intrument type": "Instrument type",
                "Instrument type": "Instrument type",
                "Transaction date": "Transaction date",
                "Volume": "Volume",
                "Unit": "Unit",
                "Price": "Price",
                "Currency": "Currency",
                "Status": "Status",
                "Details": "Details",
            }
        )

    cutoff_oldest: date | None = None

    for p in range(start_page, start_page + max_pages):
        html = fetch_page(sess, p)
        t = extract_table(html)
        if t is None or t.empty:
            print(f"[INFO] No table on page {p}. Stopping.")
            break
        t = normalize_cols(t)
        t["_pub_date"] = t["Publication date"].map(dparse)

        page_dates = sorted({d for d in t["_pub_date"].dropna().tolist()}, reverse=True)
        if page_dates:
            page_min = min(page_dates)
            page_max = max(page_dates)
            print(f"[DEBUG] Page {p} span: {page_min} .. {page_max}")
        else:
            page_min = page_max = None
            print(f"[DEBUG] Page {p} has no parsed dates")

        # uppdatera listan över topp-N publiceringsdagar (desc)
        for d in page_dates:
            if d not in unique_dates:
                unique_dates.append(d)
                unique_dates.sort(reverse=True)
                if len(unique_dates) > days:
                    unique_dates = unique_dates[:days]
        # sätt cutoff när vi har N datum
        if len(unique_dates) >= days:
            cutoff_oldest = unique_dates[-1]

        # filtrera raden lokalt på Issuer och på att datumet finns i topp-N (om vi har en cutoff)
        if issuer_sub:
            t = t[
                t["Issuer"].astype(str).str.contains(issuer_sub, case=False, na=False)
            ]
        if cutoff_oldest:
            t = t[t["_pub_date"].isin(unique_dates)]

        if not t.empty:
            collected_frames.append(t.drop(columns=["_pub_date"]))

        # om sidan redan är äldre än cutoff → stoppa
        if cutoff_oldest and page_min and page_min < cutoff_oldest:
            print(
                f"[INFO] Page {p} min date {page_min} < oldest target {cutoff_oldest}. Stopping."
            )
            break

        time.sleep(sleep)

    if not collected_frames:
        return pd.DataFrame()

    df = pd.concat(collected_frames, ignore_index=True)
    # sista säkerhetsfilter: exakt de N senaste datumen
    df["Publication date"] = pd.to_datetime(
        df["Publication date"], dayfirst=True, errors="coerce"
    ).dt.date
    if unique_dates:
        keep_set = set(unique_dates[:days])
        df = df[df["Publication date"].isin(keep_set)]
    # sortera nycklar
    df = df.sort_values(["Publication date"], ascending=False).reset_index(drop=True)
    return df


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True, help="CSV-utfil")
    ap.add_argument("--issuer", default=None, help="(valfritt) substring i Issuer")
    ap.add_argument(
        "--days",
        type=int,
        default=3,
        help="Antal senaste publiceringsdagar (default 3)",
    )
    ap.add_argument(
        "--start_page", type=int, default=1, help="Starta från denna sida (resume)"
    )
    ap.add_argument(
        "--max_pages", type=int, default=40, help="Max antal sidor att hämta"
    )
    ap.add_argument(
        "--sleep", type=float, default=0.7, help="Sek vila mellan sidladdningar"
    )
    args = ap.parse_args()

    df = scrape_last_n_publication_days(
        args.days, args.issuer, args.start_page, args.max_pages, args.sleep
    )
    if df.empty:
        print("No rows found for the latest days.")
        pd.DataFrame().to_csv(args.out, index=False)
        return

    df.to_csv(args.out, index=False)
    print(
        f"Saved {len(df)} rows across the latest {args.days} publication day(s) -> {args.out}"
    )


if __name__ == "__main__":
    main()
