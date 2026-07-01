#!/usr/bin/env python3
"""Fetch US politician (House + Senate) stock trades from Capitol Trades public API."""

import logging
from datetime import date, timedelta

import requests

logger = logging.getLogger(__name__)

_BASE = "https://api.capitoltrades.com"
_PAGE_SIZE = 100  # single page — keeps memory use flat on Pi


def fetch_trades(days: int = 3, user_agent: str | None = None) -> list[dict]:
    """Return politician trades (buys + sells) from the last `days` days."""
    since = (date.today() - timedelta(days=days)).isoformat()
    headers = {"User-Agent": user_agent or "Schedule4 Politician Monitor"}
    params = {"txDate_gte": since, "pageSize": _PAGE_SIZE, "page": 1}

    try:
        resp = requests.get(f"{_BASE}/trades", params=params, headers=headers, timeout=30)
        resp.raise_for_status()
    except requests.RequestException as e:
        logger.error("Capitol Trades request failed: %s", e)
        return []

    trades = resp.json().get("data", [])
    result = []
    for t in trades:
        pol = t.get("politician") or {}
        issuer = t.get("issuer") or {}
        ticker = issuer.get("ticker") or ""
        if not ticker:
            continue
        result.append({
            "tx_id":       t.get("_txId") or "",
            "politician":  f"{pol.get('firstName', '')} {pol.get('lastName', '')}".strip(),
            "party":       (pol.get("party") or "")[:1].upper(),  # D/R/I
            "chamber":     (pol.get("chamber") or "")[:1].upper(),  # S/H
            "state":       pol.get("state") or "",
            "ticker":      ticker,
            "issuer":      issuer.get("name") or "",
            "tx_date":     t.get("txDate") or "",
            "filing_date": t.get("filingDate") or "",
            "tx_type":     (t.get("txType") or "").lower(),  # buy / sell
            "tx_type_ext": t.get("txTypeExtended") or "",
            "amount":      (t.get("amounts") or {}).get("range") or "",
            "asset_type":  t.get("assetType") or "",
        })

    logger.info("Capitol Trades: %d trades since %s", len(result), since)
    return result
