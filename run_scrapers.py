#!/usr/bin/env python3
# Orchestrates the existing scrapers without modifying them.
# schedule4_corporate_buys.py -> US out.csv
# schedule4_corporate_buys_fi.py -> FI out_fi.csv

import argparse, subprocess, sys, shlex, os

def main():
    ap = argparse.ArgumentParser(description="Run both scrapers (US SEC + FI PDMR).")
    ap.add_argument("--us_csv", default="out.csv", help="Output CSV for US scraper (passed as --csv).")
    ap.add_argument("--fi_out", default="out_fi.csv", help="Output CSV for FI scraper (passed as --out).")
    ap.add_argument("--us_days", type=int, default=3, help="Days for US index lookback (passed as --days).")
    ap.add_argument("--fi_days", type=int, default=3, help="Latest FI publication days (passed as --days).")
    ap.add_argument("--fi_issuer", default=None, help="Optional FI issuer filter (passed as --issuer).")
    ap.add_argument("--user_agent", default=None, help="Optional SEC User-Agent string (US scraper).")
    ap.add_argument("--print_passed", action="store_true", help="Pass through to US scraper.")
    ap.add_argument("--keep_otc", action="store_true", help="Pass through to US scraper (--keep_otc).")
    ap.add_argument("--no_tenpct_filter", action="store_true", help="Pass through to US scraper.")
    ap.add_argument("--sleep", type=float, default=None, help="Pass through to US scraper (--sleep).")
    ap.add_argument("--include_codes", default=None, help="Pass through to US scraper (--include_codes).")
    args = ap.parse_args()

    cmds = []

    # US scraper
    us_cmd = [
        sys.executable, "schedule4_corporate_buys.py",
        "--csv", args.us_csv,
        "--days", str(args.us_days),
    ]
    if args.user_agent: us_cmd += ["--user_agent", args.user_agent]
    if args.print_passed: us_cmd += ["--print_passed"]
    if args.keep_otc: us_cmd += ["--keep_otc"]
    if args.no_tenpct_filter: us_cmd += ["--no_tenpct_filter"]
    if args.sleep is not None: us_cmd += ["--sleep", str(args.sleep)]
    if args.include_codes: us_cmd += ["--include_codes", args.include_codes]
    cmds.append(us_cmd)

    # FI scraper
    fi_cmd = [
        sys.executable, "schedule4_corporate_buys_fi.py",
        "--out", args.fi_out,
        "--days", str(args.fi_days),
    ]
    if args.fi_issuer: fi_cmd += ["--issuer", args.fi_issuer]
    cmds.append(fi_cmd)

    for cmd in cmds:
        print(">>>", " ".join(shlex.quote(x) for x in cmd), flush=True)
        r = subprocess.run(cmd)
        if r.returncode != 0:
            sys.exit(r.returncode)

if __name__ == "__main__":
    main()
