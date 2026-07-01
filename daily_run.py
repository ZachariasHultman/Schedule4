#!/usr/bin/env python3
"""
daily_run.py

Full Schedule4 pipeline:
  1. Scrape US (SEC Form 4) + FI (PDMR) insider filings
  2. Flag coordinated buys in both datasets
  3. Email new coordinated buys found since the previous run

Configure via config.yaml.
Credentials via .env or environment: SMTP_USER, SMTP_PASSWORD.
"""

import csv
import json
import logging
import os
import smtplib
import subprocess
import sys
from datetime import date, datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path

import yaml

from schedule4_politician_buys import fetch_trades as fetch_politician_trades

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s: %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger("daily_run")

HERE = Path(__file__).resolve().parent


# ---- Config / env ----

def load_config() -> dict:
    with open(HERE / "config.yaml", encoding="utf-8") as f:
        return yaml.safe_load(f)


def load_dotenv():
    env_file = HERE / ".env"
    if not env_file.exists():
        return
    with open(env_file) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip())


# ---- Pipeline steps ----

def run_step(cmd: list, label: str):
    logger.info("Running: %s", label)
    r = subprocess.run([str(c) for c in cmd], cwd=HERE)
    if r.returncode != 0:
        logger.error("%s failed (exit %d)", label, r.returncode)
        sys.exit(r.returncode)


# ---- Result readers ----

def _read_coordinated_us(path: Path) -> list[dict]:
    """Return list of coordinated US row dicts (one per insider × transaction)."""
    if not path.exists():
        return []
    rows = []
    with open(path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            if str(row.get("coordinated", "")).lower() in ("true", "1"):
                rows.append(row)
    return rows


def _read_coordinated_fi(path: Path) -> list[dict]:
    """Return list of coordinated FI row dicts."""
    if not path.exists():
        return []
    rows = []
    with open(path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            if str(row.get("coordinated", "")).lower() in ("true", "1"):
                rows.append(row)
    return rows


def _key_us(row: dict) -> tuple:
    return (
        row.get("issuer", "").strip(),
        row.get("filing_date", "").strip(),
        row.get("buyer", "").strip(),
    )


def _key_pol(row: dict) -> tuple:
    return (row.get("tx_id") or "", row.get("tx_date") or "")


def _key_fi(row: dict) -> tuple:
    pub = (row.get("pub_date") or row.get("Publication date", "")).strip()
    buyer = (row.get("buyer") or row.get("PDMR", "")).strip()
    issuer = (row.get("issuer") or row.get("Issuer", "")).strip()
    return (issuer, pub, buyer)


# ---- State ----

def load_state(state_file: Path) -> dict:
    if not state_file.exists():
        return {"us": set(), "fi": set(), "pol": set()}
    with open(state_file) as f:
        raw = json.load(f)
    return {
        "us":  {tuple(t) for t in raw.get("us", [])},
        "fi":  {tuple(t) for t in raw.get("fi", [])},
        "pol": {tuple(t) for t in raw.get("pol", [])},
    }


def save_state(state_file: Path, us_keys: set, fi_keys: set, pol_keys: set):
    with open(state_file, "w") as f:
        json.dump(
            {
                "us":  sorted(list(t) for t in us_keys),
                "fi":  sorted(list(t) for t in fi_keys),
                "pol": sorted(list(t) for t in pol_keys),
            },
            f, indent=2,
        )


# ---- Email ----

def _fmt_us_rows(rows: list[dict]) -> str:
    """Format US coordinated rows as a readable table."""
    if not rows:
        return "  (none)\n"
    # Group by issuer+date to show as a cluster
    groups: dict[tuple, list] = {}
    for r in rows:
        k = (r.get("ticker", ""), r.get("issuer", ""), r.get("filing_date", ""))
        groups.setdefault(k, []).append(r)

    lines = []
    for (ticker, issuer, fdate), grp in sorted(groups.items()):
        n_buyers = grp[0].get("coordinated_buyers", len(grp))
        span = grp[0].get("coord_span_abs", "")
        span_str = f"  price span ${float(span):.4f}" if span else ""
        lines.append(f"  {ticker:<6}  {issuer:<35}  filed {fdate}  ({n_buyers} buyers{span_str})")
        seen_buyers: set[str] = set()
        url = ""
        for r in grp:
            buyer = r.get("buyer", "")
            if buyer in seen_buyers:
                continue
            seen_buyers.add(buyer)
            price = r.get("price") or r.get("price_avg_from_note", "")
            shares = r.get("shares", "")
            try:
                price_str = f"${float(price):.2f}" if price else "price N/A"
            except (ValueError, TypeError):
                price_str = str(price) or "price N/A"
            try:
                shares_str = f"{int(float(shares)):,} sh" if shares else ""
            except (ValueError, TypeError):
                shares_str = str(shares)
            url = r.get("accession_url", "") or url
            lines.append(f"    → {buyer:<35}  {price_str:<12}  {shares_str}")
        if url:
            lines.append(f"    SEC: {url}")
        lines.append("")
    return "\n".join(lines)


def _fmt_fi_rows(rows: list[dict]) -> str:
    """Format FI coordinated rows as a readable table."""
    if not rows:
        return "  (none)\n"
    groups: dict[tuple, list] = {}
    for r in rows:
        instrument = r.get("instrument", r.get("Instrument", ""))
        issuer = r.get("issuer", r.get("Issuer", ""))
        pub = r.get("pub_date", r.get("Publication date", ""))
        currency = r.get("currency", r.get("Currency", ""))
        k = (instrument, issuer, pub, currency)
        groups.setdefault(k, []).append(r)

    lines = []
    for (instrument, issuer, pub, currency), grp in sorted(groups.items()):
        n_buyers = grp[0].get("coordinated_buyers", len(grp))
        span = grp[0].get("coord_span_abs", "")
        span_str = f"  price span {float(span):.4f} {currency}" if span else ""
        lines.append(f"  {instrument:<10}  {issuer:<35}  pub {pub}  ({n_buyers} buyers{span_str})")
        seen_buyers: set[str] = set()
        for r in grp:
            buyer = r.get("buyer", r.get("PDMR", ""))
            if buyer in seen_buyers:
                continue
            seen_buyers.add(buyer)
            price = r.get("price", r.get("Price", ""))
            volume = r.get("volume", r.get("Volume", ""))
            try:
                price_str = f"{float(price):.2f} {currency}" if price else "price N/A"
            except (ValueError, TypeError):
                price_str = str(price) or "price N/A"
            try:
                vol_str = f"{int(float(volume)):,}" if volume else ""
            except (ValueError, TypeError):
                vol_str = str(volume)
            lines.append(f"    → {buyer:<35}  {price_str:<18}  {vol_str}")
        lines.append("")
    return "\n".join(lines)


def _fmt_pol_rows(rows: list[dict]) -> str:
    if not rows:
        return "  (none)\n"
    buys  = [r for r in rows if r.get("tx_type") == "buy"]
    sells = [r for r in rows if r.get("tx_type") != "buy"]
    lines = []
    for label, group in (("Buys", buys), ("Sells", sells)):
        if not group:
            continue
        lines.append(f"  {label}:")
        for r in sorted(group, key=lambda x: x.get("tx_date", ""), reverse=True):
            pol   = r.get("politician", "")
            tag   = f"{r.get('party','')}-{r.get('chamber','')}/{r.get('state','')}"
            tkr   = r.get("ticker", "")
            amt   = r.get("amount", "")
            tdate = r.get("tx_date", "")
            ext   = r.get("tx_type_ext", "")
            lines.append(f"    {tkr:<6}  {pol:<25}  {tag:<10}  {amt:<20}  {tdate}  {ext}")
        lines.append("")
    return "\n".join(lines)


def build_email(
    new_us: list[dict], new_fi: list[dict], new_pol: list[dict],
    all_us: list[dict], all_fi: list[dict], all_pol: list[dict],
    scraper_status: dict,
) -> tuple[str, str]:
    today = date.today().isoformat()
    n_new_insider = len({_key_us(r) for r in new_us}) + len({_key_fi(r) for r in new_fi})
    n_new_pol = len({_key_pol(r) for r in new_pol})
    n_new = n_new_insider + n_new_pol

    if n_new:
        subject = f"Schedule4 [{today}]: {n_new} new event(s)"
    else:
        subject = f"Schedule4 [{today}]: nothing new"

    sep = "=" * 55
    thin = "-" * 55
    lines: list[str] = []

    lines += [f"Schedule4 — Insider & Politician Trades — {today}", sep, ""]

    # STATUS
    lines += ["STATUS", ""]
    for name, status in scraper_status.items():
        icon = "✓" if status.startswith("ok") or status.startswith("✓") else "✗"
        lines.append(f"  {name:<18} {icon}  {status}")
    lines += ["", thin, ""]

    # NEW SINCE LAST RUN
    new_us_keys = {_key_us(r) for r in new_us}
    new_fi_keys = {_key_fi(r) for r in new_fi}
    new_pol_keys = {_key_pol(r) for r in new_pol}
    lines += [f"NEW SINCE LAST RUN  ({n_new} event(s))", ""]

    lines.append(f"US insider — {len(new_us_keys)} new coordinated cluster(s):")
    lines.append(_fmt_us_rows(new_us))

    lines.append(f"FI insider — {len(new_fi_keys)} new coordinated cluster(s):")
    lines.append(_fmt_fi_rows(new_fi))

    lines.append(f"Politicians — {n_new_pol} new trade(s):")
    lines.append(_fmt_pol_rows(new_pol))

    lines += [thin, ""]

    # ALL TODAY
    all_us_keys = {_key_us(r) for r in all_us}
    all_fi_keys = {_key_fi(r) for r in all_fi}
    n_all_us = len({(r.get("issuer"), r.get("filing_date")) for r in all_us})
    n_all_fi = len({(r.get("issuer", r.get("Issuer")), r.get("pub_date", r.get("Publication date"))) for r in all_fi})
    lines += ["ALL TRADES IN WINDOW", ""]

    lines.append(f"US insider — {n_all_us} issuer-date cluster(s), {len(all_us_keys)} buyer row(s):")
    lines.append(_fmt_us_rows(all_us))

    lines.append(f"FI insider — {n_all_fi} issuer-date cluster(s), {len(all_fi_keys)} buyer row(s):")
    lines.append(_fmt_fi_rows(all_fi))

    lines.append(f"Politicians — {len(all_pol)} trade(s) in window:")
    lines.append(_fmt_pol_rows(all_pol))

    lines += [thin, f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}"]

    return subject, "\n".join(lines)


def send_email(cfg: dict, subject: str, body: str):
    email_cfg = cfg.get("email", {})
    to_addr = email_cfg.get("to")
    from_addr = email_cfg.get("from")
    smtp_host = email_cfg.get("smtp_host", "smtp.gmail.com")
    smtp_port = int(email_cfg.get("smtp_port", 587))

    smtp_user = os.environ.get("SMTP_USER")
    smtp_password = os.environ.get("SMTP_PASSWORD")
    if not smtp_user or not smtp_password:
        raise RuntimeError("SMTP_USER and SMTP_PASSWORD must be set (check .env)")

    msg = MIMEMultipart()
    msg["From"] = from_addr
    msg["To"] = to_addr
    msg["Subject"] = subject
    msg.attach(MIMEText(body, "plain", "utf-8"))

    with smtplib.SMTP(smtp_host, smtp_port) as srv:
        srv.starttls()
        srv.login(smtp_user, smtp_password)
        srv.sendmail(from_addr, [to_addr], msg.as_string())
    logger.info("Email sent to %s", to_addr)


# ---- Main ----

def main():
    load_dotenv()
    cfg = load_config()

    scraper_cfg = cfg.get("scraper", {})
    paths_cfg = cfg.get("paths", {})

    us_csv = HERE / paths_cfg.get("us_csv", "out.csv")
    fi_csv = HERE / paths_cfg.get("fi_csv", "out_fi.csv")
    state_file = HERE / paths_cfg.get("state_file", ".state.json")

    days = str(scraper_cfg.get("days", 1))
    user_agent = scraper_cfg.get("user_agent")
    include_codes = scraper_cfg.get("include_codes", "P,C")
    send_always = cfg.get("email", {}).get("send_always", False)
    pol_days = int(cfg.get("politician", {}).get("days", 3))

    # 1) Scrape insiders
    scrape_cmd = [
        sys.executable, "run_scrapers.py",
        "--us_csv", us_csv,
        "--fi_out", fi_csv,
        "--us_days", days,
        "--fi_days", days,
        "--include_codes", include_codes,
    ]
    if user_agent:
        scrape_cmd += ["--user_agent", user_agent]
    run_step(scrape_cmd, "scraper")

    # 2) Flag coordinated buys
    run_step([
        sys.executable, "run_coordinated_flagging.py",
        "--us_in", us_csv,
        "--fi_in", fi_csv,
    ], "coordinated flagging")

    # 3) Fetch politician trades in memory (no CSV)
    logger.info("Fetching politician trades (last %d days)", pol_days)
    all_pol = fetch_politician_trades(days=pol_days, user_agent=user_agent)
    pol_status = f"ok — {len(all_pol)} trade(s) fetched" if all_pol is not None else "failed"

    # 4) Load results and compare with previous state
    prev = load_state(state_file)
    all_us = _read_coordinated_us(us_csv)
    all_fi = _read_coordinated_fi(fi_csv)

    curr_us_keys = {_key_us(r) for r in all_us}
    curr_fi_keys = {_key_fi(r) for r in all_fi}
    curr_pol_keys = {_key_pol(r) for r in all_pol}

    new_us  = [r for r in all_us  if _key_us(r)  not in prev["us"]]
    new_fi  = [r for r in all_fi  if _key_fi(r)  not in prev["fi"]]
    new_pol = [r for r in all_pol if _key_pol(r) not in prev["pol"]]

    n_new_clusters_us = len({_key_us(r) for r in new_us})
    n_new_clusters_fi = len({_key_fi(r) for r in new_fi})

    scraper_status = {
        "US insider":    f"ok — {len(all_us)} coordinated row(s) found",
        "FI insider":    f"ok — {len(all_fi)} coordinated row(s) found",
        "new US":        f"{n_new_clusters_us} new cluster(s) vs last run",
        "new FI":        f"{n_new_clusters_fi} new cluster(s) vs last run",
        "Politicians":   pol_status,
        "new pol":       f"{len(new_pol)} new trade(s) vs last run",
    }

    logger.info("US coordinated today: %d  (new: %d clusters)", len(all_us), n_new_clusters_us)
    logger.info("FI coordinated today: %d  (new: %d clusters)", len(all_fi), n_new_clusters_fi)
    logger.info("Politician trades today: %d  (new: %d)", len(all_pol), len(new_pol))

    # 5) Email
    if new_us or new_fi or new_pol or send_always:
        subject, body = build_email(new_us, new_fi, new_pol, all_us, all_fi, all_pol, scraper_status)
        logger.info("Sending: %s", subject)
        try:
            send_email(cfg, subject, body)
        except Exception as e:
            logger.warning("Email failed: %s", e)
    else:
        logger.info("Nothing new — no email sent")

    # 6) Persist state — keep only last 30 days so the file doesn't grow forever
    from datetime import datetime, timedelta
    cutoff = (datetime.today() - timedelta(days=30)).strftime("%Y-%m-%d")

    def _prune(keys: set) -> set:
        result = set()
        for k in keys:
            date_part = k[1] if len(k) > 1 else ""
            if date_part >= cutoff:
                result.add(k)
        return result

    save_state(
        state_file,
        _prune(prev["us"] | curr_us_keys),
        _prune(prev["fi"] | curr_fi_keys),
        _prune(prev["pol"] | curr_pol_keys),
    )
    logger.info("State saved")


if __name__ == "__main__":
    logger.info("Schedule4 daily run starting")
    main()
    logger.info("Done")
