#!/usr/bin/env python3
"""
Schedule 4 Corporate Buys (Daily) -> CSV

What it does
------------
- Downloads SEC daily "By Schedule Type" index for a date or range.
- Extracts Schedule 4 / 4/A entries (index lists *.txt paths).
- Locates and parses each filing's ownership XML (schedule4.xml or primary_doc.xml).
- Filters to 10% owners, tries to keep corporate filers, keeps acquisition codes (P,C).
- Extracts price; if XML price is blank, parses footnotes for weighted avg / ranges.
- Writes CSV rows: one per (reporting person × transaction).

Usage
-----
python schedule4_corporate_buys.py --date 2025-08-12 --csv out.csv \
  --user_agent "Your Name <you@example.com>"

python schedule4_corporate_buys.py --start 2025-08-01 --end 2025-08-12 --csv out.csv \
  --user_agent "Your Name <you@example.com>"

Options (key ones)
------------------
--include_codes "P,C"    Transaction codes to include (default P,C).
--no_tenpct_filter       Include all filers (not just 10% owners).
--keep_otc               Keep OTC/foreign symbols (more noise).
--sleep 0.3              Delay between HTTP requests (seconds).
--print_passed           Print one line per kept row (live feedback).
"""

import argparse
import csv
import datetime as dt
import gzip
import re
import sys
import time
from dataclasses import dataclass
from io import BytesIO
from typing import Iterable, List, Optional, Tuple, Dict, Any
from urllib.parse import urljoin
import asyncio
import httpx
from concurrent.futures import ThreadPoolExecutor
import requests
from lxml import etree
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import random

EDGAR_ARCHIVES = "https://www.sec.gov/Archives/"
DAILY_FORM_INDEX = (
    "https://www.sec.gov/Archives/edgar/daily-index/{year}/QTR{q}/schedule.{ymd}.idx"
)

# SEC requires a real User-Agent with contact info
DEFAULT_UA = "Schedule4 Corporate Buys (your.email@example.com)"
HEADERS = {
    "User-Agent": DEFAULT_UA,
    "Accept": "*/*",
    "Accept-Encoding": "gzip, deflate",
    "Connection": "keep-alive",
}


@dataclass
class FilingRef:
    cik: str
    company_name: str
    schedule_type: str
    date_filed: str  # YYYY-MM-DD
    txt_path: str  # e.g., edgar/data/0001234567/0001234567-25-000123.txt


def quarter_of(date: dt.date) -> int:
    return (date.month - 1) // 3 + 1


def iter_dates(start: dt.date, end: dt.date) -> Iterable[dt.date]:
    d = start
    while d <= end:
        yield d
        d += dt.timedelta(days=1)


# ---- Fetch daily index (gzip-safe) ----
def fetch_daily_schedule_index(
    day: dt.date, session: requests.Session
) -> Optional[str]:
    ymd = day.strftime("%Y%m%d")
    url = DAILY_FORM_INDEX.scheduleat(year=day.year, q=quarter_of(day), ymd=ymd)
    r = session.get(url, headers=HEADERS, timeout=30)
    if r.status_code != 200:
        return None
    # If server handles gzip, requests already decoded into .text
    if r.headers.get("Content-Encoding", "").lower() == "gzip":
        return r.text
    raw = r.content
    # Some edges serve gzipped bytes w/o header; sniff magic
    if len(raw) >= 2 and raw[0] == 0x1F and raw[1] == 0x8B:
        with gzip.GzipFile(fileobj=BytesIO(raw)) as gz:
            return gz.read().decode("latin-1", errors="ignore")
    return raw.decode("latin-1", errors="ignore")


# ---- Parse fixed-width index ----
# Example row (fixed-width columns):
# 4/A            SOME COMPANY INC.                                   0000123456  20250812    edgar/data/123456/0000123456-25-000123.txt
FORM4_ROW = re.compile(
    r"^(?P<schedule>4(?:/\w+)?)\s{2,}"
    r"(?P<company>.+?)\s{2,}"
    r"(?P<cik>\d{7,10})\s{2,}"
    r"(?P<date>\d{8})\s{2,}"
    r"(?P<file>edgar/data/\d+/\d{10}-\d{2}-\d{6}\.txt)\s*$",
    re.MULTILINE,
)


def parse_schedule_index(idx_text: str) -> List[FilingRef]:
    out: List[FilingRef] = []
    for m in FORM4_ROW.finditer(idx_text):
        ft = m.group("schedule").upper()
        if not (ft == "4" or ft.startswith("4/")):  # includes 4/A
            continue
        date = m.group("date")
        out.append(
            FilingRef(
                cik=m.group("cik"),
                company_name=m.group("company").strip(),
                schedule_type=m.group("schedule").strip(),
                date_filed=f"{date[0:4]}-{date[4:6]}-{date[6:8]}",
                txt_path=m.group("file"),
            )
        )
    return out


# ---- From .txt to XML candidates ----
def xml_candidates_from_txt(txt_path: str) -> Tuple[str, List[str]]:
    # txt_path: edgar/data/CIK/ACCESSION.txt  → dir: edgar/data/CIK/ACCESSION/
    base_dir = txt_path.rsplit("/", 1)[0] + "/"
    acc = txt_path.rsplit("/", 1)[1].replace(".txt", "")
    dir_url = urljoin(EDGAR_ARCHIVES, base_dir + acc + "/")
    index_url = dir_url + f"{acc}-index.htm"
    return index_url, [dir_url + "schedule4.xml", dir_url + "primary_doc.xml"]


def fetch_xml(url: str, session: requests.Session) -> Optional[bytes]:
    # Separate connect/read timeouts; SEC can be slow on read
    connect_t, read_t = 5, 60
    tries = 4
    for i in range(tries):
        try:
            r = session.get(url, headers=HEADERS, timeout=(connect_t, read_t))
            if (
                r.status_code == 200
                and "xml" in r.headers.get("Content-Type", "").lower()
            ):
                return r.content
            # 404 or wrong content: no point retrying further
            if r.status_code == 404:
                return None
        except (requests.exceptions.ReadTimeout, requests.exceptions.ConnectTimeout):
            # exponential backoff with jitter
            time.sleep((2**i) * 0.5 + random.random() * 0.2)
            continue
        except requests.exceptions.RequestException:
            # transient network error → backoff and retry
            time.sleep((2**i) * 0.5 + random.random() * 0.2)
            continue
        break
    return None


def find_xml_via_index(index_url: str, session: requests.Session) -> Optional[str]:
    try:
        r = session.get(index_url, headers=HEADERS, timeout=(5, 60))
    except requests.exceptions.RequestException:
        return None
    if r.status_code != 200:
        return None
    for href in re.findall(r'href="([^"]+\.xml)"', r.text, flags=re.IGNORECASE):
        xml_url = urljoin(index_url, href)
        xb = fetch_xml(xml_url, session)
        if xb and b"<ownershipDocument" in xb:
            return xml_url
    return None


# ---- Footnote price parsing ----
MONEY_RE = re.compile(r"\$?\s*([0-9]{1,3}(?:,[0-9]{3})*(?:\.[0-9]+)?)")
RANGE_RE = re.compile(
    r"from\s*\$?\s*([0-9]{1,3}(?:,[0-9]{3})*(?:\.[0-9]+)?)\s*to\s*\$?\s*([0-9]{1,3}(?:,[0-9]{3})*(?:\.[0-9]+)?)",
    re.IGNORECASE,
)
WEIGHTED_AVG_RE = re.compile(
    r"weighted average (?:price|purchase price)\s*of\s*\$?\s*([0-9]{1,3}(?:,[0-9]{3})*(?:\.[0-9]+)?)",
    re.IGNORECASE,
)


def parse_price_from_text(text: str) -> Dict[str, Optional[float]]:
    text = text.replace("\u00a0", " ")
    out = {
        "price_avg_from_note": None,
        "price_min_from_note": None,
        "price_max_from_note": None,
    }
    m = RANGE_RE.search(text)
    if m:
        lo = float(m.group(1).replace(",", ""))
        hi = float(m.group(2).replace(",", ""))
        out.update(
            {
                "price_min_from_note": lo,
                "price_max_from_note": hi,
                "price_avg_from_note": (lo + hi) / 2.0,
            }
        )
        return out
    m = WEIGHTED_AVG_RE.search(text)
    if m:
        out["price_avg_from_note"] = float(m.group(1).replace(",", ""))
        return out
    m = MONEY_RE.search(text)
    if m:
        out["price_avg_from_note"] = float(m.group(1).replace(",", ""))
    return out


def collect_footnotes(root: etree._Element) -> Dict[str, str]:
    return {
        fn.get("id"): "".join(fn.itertext()).strip()
        for fn in root.xpath("//footnotes/footnote")
        if fn.get("id")
    }


# ---- XML parse ----
def parse_schedule4_xml(xml_bytes: bytes) -> Tuple[dict, List[dict]]:
    root = etree.fromstring(xml_bytes)
    xp = lambda p: root.xpath(p, namespaces=root.nsmap)

    header = {
        "issuerName": "".join(xp("string(//issuer/issuerName)")).strip(),
        "issuerTradingSymbol": "".join(
            xp("string(//issuer/issuerTradingSymbol)")
        ).strip(),
        "periodOfReport": "".join(xp("string(//periodOfReport)")).strip(),
    }
    footnotes = collect_footnotes(root)
    owners = xp("//reportingOwner")
    tx_nodes = xp("//nonDerivativeTable/nonDerivativeTransaction")

    rows = []
    for tx in tx_nodes:
        code = "".join(tx.xpath("string(transactionCoding/transactionCode)")).strip()
        tdate = "".join(tx.xpath("string(transactionDate/value)")).strip()
        shares = "".join(
            tx.xpath("string(transactionAmounts/transactionShares/value)")
        ).strip()
        price = "".join(
            tx.xpath("string(transactionAmounts/transactionPricePerShare/value)")
        ).strip()

        tx_note_ids = [n.get("id") for n in tx.xpath(".//footnoteId[@id]")]
        parsed_note = {
            "price_avg_from_note": None,
            "price_min_from_note": None,
            "price_max_from_note": None,
        }
        if not price:
            texts = [footnotes.get(fid, "") for fid in tx_note_ids if fid in footnotes]
            remarks = "".join(xp("string(//remarks)")).strip()
            if remarks:
                texts.append(remarks)
            for txt in texts:
                pn = parse_price_from_text(txt)
                if (
                    pn["price_avg_from_note"] is not None
                    or pn["price_min_from_note"] is not None
                ):
                    parsed_note = pn
                    break

        for o in owners:
            name = "".join(o.xpath("string(reportingOwnerId/rptOwnerName)")).strip()
            ten = (
                "".join(o.xpath("string(reportingOwnerRelationship/isTenPercentOwner)"))
                .strip()
                .lower()
                == "true"
            )
            rows.append(
                {
                    "rptOwnerName": name,
                    "isTenPercentOwner": ten,
                    "transactionCode": code,
                    "transactionDate": tdate,
                    "transactionShares": shares,
                    "transactionPricePerShare": price,
                    "price_avg_from_note": parsed_note["price_avg_from_note"],
                    "price_min_from_note": parsed_note["price_min_from_note"],
                    "price_max_from_note": parsed_note["price_max_from_note"],
                }
            )
    return header, rows


# ---- Filters ----
CORP_SUFFIX_RE = re.compile(
    r"\b(inc\.?|corporation|corp\.?|ltd\.?|plc|ag|nv|s\.a\.|gmbh|holdings?|group|co\.?)\b",
    re.IGNORECASE,
)
INDIVIDUAL_NAME_RE = re.compile(r"^[A-Z][a-z]+(?:\s+[A-Z]\.)?(?:\s+[A-Z][a-z]+){1,2}$")


def likely_corporate_name(name: str) -> bool:
    if CORP_SUFFIX_RE.search(name):
        return True
    if re.search(r"\b(LP|LLC|LLP|L\.P\.|L\.L\.C\.)\b", name, re.IGNORECASE):
        return True
    if len(name.split()) >= 3 and name.isupper():
        return True
    return False


def likely_individual_name(name: str) -> bool:
    return bool(INDIVIDUAL_NAME_RE.match(name)) and not likely_corporate_name(name)


def filter_transactions(
    header: dict,
    txs: List[dict],
    allowed_codes: set,
    tenpct_required: bool,
    drop_otc: bool,
) -> List[dict]:
    out = []
    symbol = (header.get("issuerTradingSymbol") or "").strip()
    if drop_otc:
        if not symbol or "." in symbol or len(symbol) > 6:
            return out
    for t in txs:
        if tenpct_required and not t["isTenPercentOwner"]:
            continue
        if likely_individual_name(t["rptOwnerName"]) and not likely_corporate_name(
            t["rptOwnerName"]
        ):
            continue
        if t["transactionCode"] not in allowed_codes:
            continue
        out.append(t)
    return out


# central rate limiter (tokens per second)
class RateLimiter:
    def __init__(self, rps=2.0):
        self.permit_interval = 1.0 / rps
        self._next = time.monotonic()
        self._lock = asyncio.Lock()

    async def acquire(self):
        async with self._lock:
            now = time.monotonic()
            if now < self._next:
                await asyncio.sleep(self._next - now)
            self._next = max(now, self._next) + self.permit_interval


async def fetch_xml_httpx(client, url, rl: RateLimiter):
    await rl.acquire()
    r = await client.get(url, timeout=30)
    if r.status_code == 200 and "xml" in r.headers.get("content-type", "").lower():
        return r.content
    return None


async def find_xml_via_index_httpx(client, index_url, rl: RateLimiter):
    await rl.acquire()
    r = await client.get(index_url, timeout=30)
    if r.status_code != 200:
        return None, None
    for href in re.findall(r'href="([^"]+\.xml)"', r.text, flags=re.I):
        url = httpx.URL(index_url).join(href)
        xb = await fetch_xml_httpx(client, str(url), rl)
        if xb and b"<ownershipDocument" in xb:
            return str(url), xb
    return None, None


async def process_filing_async(
    client, rl, filing, allowed_codes, tenpct_required, drop_otc, print_passed
):
    index_url, xml_try = xml_candidates_from_txt(filing.txt_path)
    xml_bytes, xml_url_final = None, None
    # fast path
    for cand in xml_try:
        xb = await fetch_xml_httpx(client, cand, rl)
        if xb:
            xml_bytes, xml_url_final = xb, cand
            break
    if xml_bytes is None:
        found_url, xb = await find_xml_via_index_httpx(client, index_url, rl)
        if xb:
            xml_bytes, xml_url_final = xb, found_url
    if xml_bytes is None:
        return 0, 0  # kept, raw

    header, txs = parse_schedule4_xml(xml_bytes)
    filtered = filter_transactions(
        header, txs, allowed_codes, tenpct_required, drop_otc
    )
    kept = 0
    # return CSV rows to be written by the main thread
    rows = []
    for t in filtered:
        price = t["transactionPricePerShare"] or (
            t["price_avg_from_note"] if t["price_avg_from_note"] is not None else ""
        )
        rows.append(
            {
                "buyer": t["rptOwnerName"],
                "issuer": header.get("issuerName", ""),
                "ticker": header.get("issuerTradingSymbol", ""),
                "trade_date": t["transactionDate"] or header.get("periodOfReport", ""),
                "filing_date": filing.date_filed,
                "price": price,
                "price_min_from_note": t["price_min_from_note"] or "",
                "price_max_from_note": t["price_max_from_note"] or "",
                "shares": t["transactionShares"],
                "transaction_code": t["transactionCode"],
                "accession_url": urljoin(
                    EDGAR_ARCHIVES,
                    filing.txt_path.rsplit("/", 1)[0]
                    + "/"
                    + filing.txt_path.rsplit("/", 1)[1].replace(".txt", ""),
                ),
                "xml_url": xml_url_final or "",
            }
        )
        kept += 1
        if print_passed:
            print(
                f"PASS | {header.get('issuerTradingSymbol','')} {header.get('issuerName','')} <- {t['rptOwnerName']} [{t['transactionCode']}]"
            )
    return kept, len(txs), rows


def process_date_parallel(
    day,
    filings,
    writer,
    allowed_codes,
    tenpct_required,
    drop_otc,
    print_passed,
    rps=2.0,
    workers=6,
):
    kept_total = 0
    raw_total = 0
    rows_to_write = []

    async def runner():
        nonlocal kept_total, raw_total, rows_to_write
        rl = RateLimiter(rps=rps)
        limits = httpx.Limits(
            max_keepalive_connections=workers, max_connections=workers
        )
        async with httpx.AsyncClient(
            http2=True, headers=HEADERS, limits=limits
        ) as client:
            tasks = [
                process_filing_async(
                    client,
                    rl,
                    f,
                    allowed_codes,
                    tenpct_required,
                    drop_otc,
                    print_passed,
                )
                for f in filings
            ]
            for coro in asyncio.as_completed(tasks):
                kept, raw, rows = await coro
                kept_total += kept
                raw_total += raw
                if rows:
                    rows_to_write.extend(rows)

    asyncio.run(runner())
    # single-writer flush
    for row in rows_to_write:
        writer.writerow(row)
    return kept_total, raw_total


# ---- Per-day processing ----
def process_date(
    day: dt.date,
    writer: csv.DictWriter,
    session: requests.Session,
    sleep_s: float,
    allowed_codes: set,
    tenpct_required: bool,
    drop_otc: bool,
    print_passed: bool,
) -> Tuple[int, int]:
    idx_text = fetch_daily_schedule_index(day, session)
    if not idx_text:
        return (0, 0)

    filings = parse_schedule_index(idx_text)
    kept = 0
    raw = 0

    for f in filings:
        index_url, xml_try = xml_candidates_from_txt(f.txt_path)

        xml_bytes = None
        xml_url_final = None
        for cand in xml_try:
            xb = fetch_xml(cand, session)
            if xb:
                xml_bytes = xb
                xml_url_final = cand
                break
        if xml_bytes is None:
            found = find_xml_via_index(index_url, session)
            if found:
                xb = fetch_xml(found, session)
                if xb:
                    xml_bytes = xb
                    xml_url_final = found
        if xml_bytes is None:
            time.sleep(sleep_s)
            continue

        header, txs = parse_schedule4_xml(xml_bytes)
        for t in txs:
            print(
                f"DEBUG {day} {header.get('issuerTradingSymbol')} <- {t['rptOwnerName']} [{t['transactionCode']}]"
            )
        raw += len(txs)
        filtered = filter_transactions(
            header, txs, allowed_codes, tenpct_required, drop_otc
        )
        for t in filtered:
            price = t["transactionPricePerShare"] or (
                t["price_avg_from_note"] if t["price_avg_from_note"] is not None else ""
            )
            writer.writerow(
                {
                    "buyer": t["rptOwnerName"],
                    "issuer": header.get("issuerName", ""),
                    "ticker": header.get("issuerTradingSymbol", ""),
                    "trade_date": t["transactionDate"]
                    or header.get("periodOfReport", ""),
                    "filing_date": f.date_filed,
                    "price": price,
                    "price_min_from_note": t["price_min_from_note"] or "",
                    "price_max_from_note": t["price_max_from_note"] or "",
                    "shares": t["transactionShares"],
                    "transaction_code": t["transactionCode"],
                    "accession_url": urljoin(
                        EDGAR_ARCHIVES,
                        f.txt_path.rsplit("/", 1)[0]
                        + "/"
                        + f.txt_path.rsplit("/", 1)[1].replace(".txt", ""),
                    ),
                    "xml_url": xml_url_final or "",
                }
            )
            kept += 1
            if print_passed:
                # concise live feedback per kept row
                print(
                    f"PASS {day} | {header.get('issuerTradingSymbol','')} {header.get('issuerName','')} <- {t['rptOwnerName']} [{t['transactionCode']}] {t['transactionDate'] or header.get('periodOfReport','')}"
                )
        time.sleep(sleep_s)
    return (kept, raw)


def process_date_async(
    day: dt.date,
    writer: csv.DictWriter,
    session: requests.Session,
    sleep_s: float,
    allowed_codes: set,
    tenpct_required: bool,
    drop_otc: bool,
    print_passed: bool,
) -> Tuple[int, int]:
    """
    Parallel, rate-limited processing for a single day with live per-row prints.
    Uses HTTP/2 + connection reuse and a single printer coroutine so output streams continuously.
    """

    idx_text = fetch_daily_schedule_index(day, session)
    if not idx_text:
        return (0, 0)
    filings = parse_schedule_index(idx_text)
    if not filings:
        return (0, 0)

    # ---- helpers ----
    class RateLimiter:
        def __init__(self, rps: float = 2.0):
            self.permit_interval = 1.0 / max(rps, 0.1)
            self._next = time.monotonic()
            self._lock = asyncio.Lock()

        async def acquire(self):
            async with self._lock:
                now = time.monotonic()
                if now < self._next:
                    await asyncio.sleep(self._next - now)
                self._next = max(now, self._next) + self.permit_interval

    async def fetch_xml_httpx(client: httpx.AsyncClient, url: str, rl: RateLimiter):
        await rl.acquire()
        r = await client.get(url, timeout=30)
        if r.status_code == 200 and "xml" in r.headers.get("content-type", "").lower():
            return r.content
        return None

    async def find_xml_via_index_httpx(
        client: httpx.AsyncClient, index_url: str, rl: RateLimiter
    ):
        await rl.acquire()
        r = await client.get(index_url, timeout=30)
        if r.status_code != 200:
            return None, None
        for href in re.findall(r'href="([^"]+\.xml)"', r.text, flags=re.I):
            url = str(httpx.URL(index_url).join(href))
            xb = await fetch_xml_httpx(client, url, rl)
            if xb and b"<ownershipDocument" in xb:
                return url, xb
        return None, None

    async def process_filing_async(
        client: httpx.AsyncClient,
        rl: RateLimiter,
        f: FilingRef,
        q: "asyncio.Queue[str]",
    ):
        index_url, xml_try = xml_candidates_from_txt(f.txt_path)

        xml_bytes = None
        xml_url_final = None
        for cand in xml_try:
            xb = await fetch_xml_httpx(client, cand, rl)
            if xb:
                xml_bytes, xml_url_final = xb, cand
                break

        if xml_bytes is None:
            found_url, xb = await find_xml_via_index_httpx(client, index_url, rl)
            if xb:
                xml_bytes, xml_url_final = xb, found_url

        if xml_bytes is None:
            return 0, 0, []  # kept, raw, rows

        header, txs = parse_schedule4_xml(xml_bytes)
        raw = len(txs)
        filtered = filter_transactions(
            header, txs, allowed_codes, tenpct_required, drop_otc
        )

        rows = []
        kept = 0
        acc_base = urljoin(
            EDGAR_ARCHIVES,
            f.txt_path.rsplit("/", 1)[0]
            + "/"
            + f.txt_path.rsplit("/", 1)[1].replace(".txt", ""),
        )
        for t in filtered:
            price = t["transactionPricePerShare"] or (
                t["price_avg_from_note"] if t["price_avg_from_note"] is not None else ""
            )
            row = {
                "buyer": t["rptOwnerName"],
                "issuer": header.get("issuerName", ""),
                "ticker": header.get("issuerTradingSymbol", ""),
                "trade_date": t["transactionDate"] or header.get("periodOfReport", ""),
                "filing_date": f.date_filed,
                "price": price,
                "price_min_from_note": t.get("price_min_from_note") or "",
                "price_max_from_note": t.get("price_max_from_note") or "",
                "shares": t["transactionShares"],
                "transaction_code": t["transactionCode"],
                "accession_url": acc_base,
                "xml_url": xml_url_final or "",
            }
            rows.append(row)
            kept += 1
            if print_passed:
                msg = f"PASS {day} | {header.get('issuerTradingSymbol','')} {header.get('issuerName','')} <- {t['rptOwnerName']} [{t['transactionCode']}] {row['trade_date']}"
                await q.put(msg)
        return kept, raw, rows

    kept_total = 0
    raw_total = 0
    rows_to_write: List[dict] = []

    async def runner():
        nonlocal kept_total, raw_total, rows_to_write
        rl = RateLimiter(rps=2.0)  # global polite rate limit
        limits = httpx.Limits(max_keepalive_connections=8, max_connections=8)
        async with httpx.AsyncClient(
            http2=True, headers=HEADERS, limits=limits
        ) as client:
            q: asyncio.Queue[str] = asyncio.Queue()

            async def printer():
                while True:
                    msg = await q.get()
                    if msg is None:
                        break
                    print(msg, flush=True)
                    q.task_done()

            printer_task = asyncio.create_task(printer())

            async def worker(filing: FilingRef):
                nonlocal kept_total, raw_total, rows_to_write
                kept, raw, rows = await process_filing_async(client, rl, filing, q)
                kept_total += kept
                raw_total += raw
                if rows:
                    rows_to_write.extend(rows)

            tasks = [asyncio.create_task(worker(f)) for f in filings]
            await asyncio.gather(*tasks)

            if print_passed:
                await q.put(None)
                await q.join()
            await printer_task

    asyncio.run(runner())

    for row in rows_to_write:
        writer.writerow(row)

    return (kept_total, raw_total)


def make_session(user_agent: str) -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": user_agent})
    retry = Retry(
        total=6,
        connect=6,
        read=6,
        backoff_factor=0.6,  # 0.6s, 1.2s, 2.4s, ...
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "POST"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=16, pool_maxsize=16)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    return s


# ---- Main ----
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default=None, help="Single date YYYY-MM-DD")
    ap.add_argument("--start", default=None, help="Start date YYYY-MM-DD")
    ap.add_argument("--end", default=None, help="End date YYYY-MM-DD")
    ap.add_argument("--csv", required=True, help="Output CSV path")
    ap.add_argument(
        "--user_agent", default=None, help="Custom SEC User-Agent with contact info"
    )
    ap.add_argument(
        "--sleep", type=float, default=0.3, help="Delay between HTTP requests (seconds)"
    )
    ap.add_argument(
        "--include_codes",
        default="P,C",
        help="Comma list of transaction codes to include",
    )
    ap.add_argument(
        "--no_tenpct_filter",
        action="store_true",
        help="Include all filers (not just 10%% owners)",
    )
    ap.add_argument("--keep_otc", action="store_true", help="Keep OTC/foreign symbols")
    ap.add_argument(
        "--print_passed",
        action="store_true",
        help="Print one line per kept row for live feedback",
    )
    args = ap.parse_args()

    if args.user_agent:
        HEADERS["User-Agent"] = args.user_agent

    # Date resolution
    if args.date:
        start = end = dt.date.fromisoscheduleat(args.date)
    else:
        if args.start and args.end:
            start = dt.date.fromisoscheduleat(args.start)
            end = dt.date.fromisoscheduleat(args.end)
        else:
            # default: yesterday (US/Eastern ambiguity ignored)
            end = dt.date.today() - dt.timedelta(days=1)
            start = end

    allowed_codes = set(
        c.strip().upper() for c in args.include_codes.split(",") if c.strip()
    )
    tenpct_required = not args.no_tenpct_filter
    drop_otc = not args.keep_otc

    # CSV
    fieldnames = [
        "buyer",
        "issuer",
        "ticker",
        "trade_date",
        "filing_date",
        "price",
        "price_min_from_note",
        "price_max_from_note",
        "shares",
        "transaction_code",
        "accession_url",
        "xml_url",
    ]
    try:
        with open(args.csv, "x", newline="", encoding="utf-8") as f:
            csv.DictWriter(f, fieldnames=fieldnames).writeheader()
    except FileExistsError:
        pass

    s = make_session(HEADERS["User-Agent"])
    total_kept = 0
    total_raw = 0
    with open(args.csv, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        for day in iter_dates(start, end):
            kept, raw = process_date(
                day,
                w,
                s,
                args.sleep,
                allowed_codes,
                tenpct_required,
                drop_otc,
                args.print_passed,
            )
            total_kept += kept
            total_raw += raw
            print(f"{day}: scanned {raw} txs, kept {kept}")

    print(f"Done. Total kept: {total_kept} (from {total_raw} parsed transactions).")


if __name__ == "__main__":
    main()
