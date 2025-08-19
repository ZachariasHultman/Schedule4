#!/usr/bin/env python3
import argparse, subprocess, sys, shlex, os, shutil

def main():
    ap = argparse.ArgumentParser(description="Run both coordinated-flagging scripts (US + FI), writing in place by default.")
    ap.add_argument("--us_in", default="out.csv", help="Input CSV for US flagger.")
    ap.add_argument("--us_out", default=None, help="Optional output CSV for US flagger (default: in-place).")

    ap.add_argument("--fi_in", default="out_fi.csv", help="Input CSV for FI flagger.")
    ap.add_argument("--fi_out", default=None, help="Optional output CSV for FI flagger (default: in-place).")

    # aligned options passed to BOTH
    ap.add_argument("--by", choices=["publication","transaction"], default="publication")
    ap.add_argument("--abs_tol", type=float, default=0.02)
    ap.add_argument("--pct_tol", type=float, default=0.003)
    ap.add_argument("--min_buyers", type=int, default=2)

    # FI-specific toggles (dedupe policy during computation)
    ap.add_argument("--keep_history", action="store_true", help="FI only: keep Status='History' rows when computing flags.")
    ap.add_argument("--no_prefer_revised", action="store_true", help="FI only: do not prefer 'Revised' over 'Notification'.")

    args = ap.parse_args()

    cmds = []

    # US
    us_cmd = [sys.executable, "schedule4_coordinated_buys.py", "--in", args.us_in, "--by", args.by,
              "--abs_tol", str(args.abs_tol), "--pct_tol", str(args.pct_tol), "--min_buyers", str(args.min_buyers)]
    if args.us_out:
        us_cmd += ["--out", args.us_out]
    cmds.append(us_cmd)

    # FI
    fi_cmd = [sys.executable, "schedule4_coordinated_buys_fi.py", "--in", args.fi_in, "--by", args.by,
              "--abs_tol", str(args.abs_tol), "--pct_tol", str(args.pct_tol), "--min_buyers", str(args.min_buyers)]
    if args.keep_history:
        fi_cmd += ["--keep_history"]
    if args.no_prefer_revised:
        fi_cmd += ["--no_prefer_revised"]
    if args.fi_out:
        fi_cmd += ["--out", args.fi_out]
    cmds.append(fi_cmd)

    for cmd in cmds:
        print(">>>", " ".join(shlex.quote(x) for x in cmd), flush=True)
        r = subprocess.run(cmd)
        if r.returncode != 0:
            sys.exit(r.returncode)

if __name__ == "__main__":
    main()
