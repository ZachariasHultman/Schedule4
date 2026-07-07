#!/usr/bin/env python3
"""Fetch US politician (House + Senate) stock trades from the FMP API.

Requires FMP_API_KEY in the environment (free tier: 250 calls/day; this
module uses one call per chamber per run).
"""

import hashlib
import logging
import os
from datetime import date, timedelta

import requests

logger = logging.getLogger(__name__)

_BASE = "https://financialmodelingprep.com/stable"
_PAGE_SIZE = 25  # free-tier hard cap (limit>25 or page>0 returns 402)

# (endpoint, chamber code)
_ENDPOINTS = [("senate-latest", "S"), ("house-latest", "H")]


def _tx_id(row: dict) -> str:
    """Stable id — FMP rows have no transaction id of their own."""
    raw = "|".join(str(row.get(k, "")) for k in (
        "senateID", "link", "symbol", "transactionDate", "type", "amount", "owner",
    ))
    return hashlib.sha1(raw.encode()).hexdigest()[:16]


def _state(district: str, chamber: str) -> str:
    # Senate: "WV"; House: "CA31"
    return district[:2] if chamber == "H" else district


def fetch_trades(days: int = 3, user_agent: str | None = None) -> list[dict]:
    """Return politician trades (buys + sells) disclosed in the last `days` days."""
    api_key = os.environ.get("FMP_API_KEY")
    if not api_key:
        logger.error("FMP_API_KEY not set — skipping politician trades")
        return []

    since = (date.today() - timedelta(days=days)).isoformat()
    headers = {"User-Agent": user_agent or "Schedule4 Politician Monitor"}

    result = []
    for endpoint, chamber in _ENDPOINTS:
        params = {"page": 0, "limit": _PAGE_SIZE, "apikey": api_key}
        try:
            resp = requests.get(f"{_BASE}/{endpoint}", params=params,
                                headers=headers, timeout=30)
            resp.raise_for_status()
            trades = resp.json()
        except (requests.RequestException, ValueError) as e:
            logger.error("FMP %s request failed: %s", endpoint, e)
            continue
        if not isinstance(trades, list):
            logger.error("FMP %s: unexpected response: %s", endpoint, str(trades)[:200])
            continue

        for t in trades:
            if (t.get("disclosureDate") or "") < since:
                continue
            ticker = t.get("symbol") or ""
            if not ticker:
                continue
            tx_type_ext = t.get("type") or ""
            result.append({
                "tx_id":       _tx_id(t),
                "politician":  f"{t.get('firstName', '')} {t.get('lastName', '')}".strip(),
                "party":       "",  # not provided by FMP
                "chamber":     chamber,
                "state":       _state(t.get("district") or "", chamber),
                "ticker":      ticker,
                "issuer":      t.get("assetDescription") or "",
                "tx_date":     t.get("transactionDate") or "",
                "filing_date": t.get("disclosureDate") or "",
                "tx_type":     "buy" if tx_type_ext.lower().startswith("purchase") else "sell",
                "tx_type_ext": tx_type_ext,
                "amount":      t.get("amount") or "",
                "asset_type":  t.get("assetType") or "",
            })

    logger.info("FMP: %d trades disclosed since %s", len(result), since)
    return result
