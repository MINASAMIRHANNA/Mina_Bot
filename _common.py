#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import argparse, sqlite3, json, os, sys, math, datetime
from typing import List, Optional, Any

def utc_now_iso() -> str:
    return datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

def get_cols(conn: sqlite3.Connection, table: str) -> List[str]:
    rows = conn.execute(f"PRAGMA table_info({table});").fetchall()
    return [r[1] for r in rows]

def has_col(cols: List[str], name: str) -> bool:
    return name in cols

def safe_float(x, default=0.0) -> float:
    try:
        if x is None:
            return default
        if isinstance(x, (int, float)):
            return float(x)
        s = str(x).strip()
        if s == "" or s.lower() == "none":
            return default
        return float(s)
    except Exception:
        return default

def safe_int(x, default=0) -> int:
    try:
        if x is None:
            return default
        if isinstance(x, int):
            return x
        s = str(x).strip()
        if s == "" or s.lower() == "none":
            return default
        return int(float(s))
    except Exception:
        return default

def direction_from_signal(signal: str) -> Optional[int]:
    if not signal:
        return None
    s = str(signal).upper()
    if "SELL" in s or "SHORT" in s:
        return -1
    if "BUY" in s or "LONG" in s:
        return +1
    return None

def direction_from_side(side: str) -> Optional[int]:
    if not side:
        return None
    s = str(side).upper()
    if s in ("SHORT","SELL"):
        return -1
    if s in ("LONG","BUY"):
        return +1
    return None
