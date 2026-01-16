#!/usr/bin/env python3
"""
patch_main_add_parse_csv_list.py

Fixes: NameError: _parse_csv_list is not defined

What it does:
- Creates a timestamped backup: main.py.bak_YYYYMMDD_HHMMSS (UTC)
- Inserts safe helpers: _parse_csv_list(), _parse_bool(), _parse_int()
- Only patches if it detects usage of _parse_csv_list( and no definition exists

Usage:
  python patch_main_add_parse_csv_list.py main.py
"""

from __future__ import annotations

import re
import sys
from datetime import datetime, timezone
from pathlib import Path

HELPER_BLOCK = """
# ============================
# Helpers (auto-added by patch)
# ============================
def _parse_csv_list(v):
    \"\"\"Accepts None, list/tuple/set, or '1m,5m ; 15m' and returns list[str].\"\"\"
    if v is None:
        return []
    if isinstance(v, (list, tuple, set)):
        return [str(x).strip() for x in v if str(x).strip()]
    s = str(v).strip()
    if not s:
        return []
    s = s.replace(";", ",").replace("|", ",")
    parts = [p.strip() for p in s.split(",")]
    return [p for p in parts if p]

def _parse_bool(v, default=False):
    if v is None:
        return default
    s = str(v).strip().lower()
    if s in ("1", "true", "yes", "y", "on"):
        return True
    if s in ("0", "false", "no", "n", "off"):
        return False
    return default

def _parse_int(v, default=0):
    try:
        return int(v)
    except Exception:
        return default
""".lstrip("\\n")

def utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

def main() -> None:
    if len(sys.argv) < 2:
        print("Usage: python patch_main_add_parse_csv_list.py <path_to_main.py>")
        raise SystemExit(2)

    path = Path(sys.argv[1]).expanduser().resolve()
    if not path.exists():
        print(f"ERROR: file not found: {path}")
        raise SystemExit(2)

    text = path.read_text(encoding="utf-8", errors="ignore")

    uses = "_parse_csv_list(" in text
    has_def = re.search(r"^\\s*def\\s+_parse_csv_list\\s*\\(", text, flags=re.M) is not None

    if not uses:
        print("No _parse_csv_list() usage found. Nothing to do.")
        return

    if has_def:
        print("_parse_csv_list() already defined. Nothing to do.")
        return

    lines = text.splitlines(True)

    # Insert after the import block (best-effort)
    last_import = -1
    for i, ln in enumerate(lines):
        if re.match(r"^\\s*(import|from)\\s+\\w+", ln):
            last_import = i
            continue

        # once we passed imports and hit first non-import code, stop scanning
        if last_import >= 0 and ln.strip() and not re.match(r"^\\s*(import|from)\\s+", ln):
            break

    insert_at = 0
    if last_import >= 0:
        insert_at = last_import + 1
        while insert_at < len(lines) and lines[insert_at].strip() == "":
            insert_at += 1

    patched = "".join(lines[:insert_at]) + "\\n" + HELPER_BLOCK + "\\n" + "".join(lines[insert_at:])

    backup = path.with_name(path.name + f".bak_{utc_stamp()}")
    backup.write_text(text, encoding="utf-8")
    path.write_text(patched, encoding="utf-8")

    print(f"✅ Patched: {path}")
    print(f"🧾 Backup:  {backup}")
    print("Added: _parse_csv_list(), _parse_bool(), _parse_int()")

if __name__ == "__main__":
    main()
