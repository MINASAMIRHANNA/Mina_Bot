"""db_path.py

Sprint 2 (stability): DB path pinning.

Problem
- Running the project from different working directories can accidentally create multiple
  bot_data.db files (e.g., one in project root, one in ./dashboard, one in cwd).
- That leads to "dashboard shows empty data" / "bot heartbeat missing" / confusing behavior.

Solution
- Pin a single DB path using a small lock file in the project root.
- On first run, we choose the best existing DB candidate and write it to .db_path.
- Next runs always use the pinned path regardless of current working directory.

Notes
- This keeps behavior backward-compatible: if you already have a DB in a non-root location,
  the first run will detect and pin it.
- You can manually override by editing .db_path (a single line absolute path).
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import List, Optional


PROJECT_ROOT = Path(__file__).resolve().parent
PIN_FILE = PROJECT_ROOT / ".db_path"
DEFAULT_DB_NAME = "bot_data.db"


def _read_pin() -> Optional[str]:
    try:
        if PIN_FILE.exists():
            s = PIN_FILE.read_text(encoding="utf-8").strip()
            return s or None
    except Exception:
        return None
    return None


def _write_pin(p: str) -> None:
    try:
        PIN_FILE.write_text(str(p).strip() + "\n", encoding="utf-8")
    except Exception:
        pass


def find_db_files(db_name: str = DEFAULT_DB_NAME) -> List[str]:
    """Find existing bot_data.db files in likely locations."""
    candidates = [
        PROJECT_ROOT / db_name,
        PROJECT_ROOT.parent / db_name,
        (PROJECT_ROOT / "dashboard") / db_name,
        Path(os.getcwd()) / db_name,
    ]
    found: List[str] = []
    for p in candidates:
        try:
            if p.exists():
                rp = str(p.resolve())
                if rp not in found:
                    found.append(rp)
        except Exception:
            continue
    return found


def get_db_path(db_name: str = DEFAULT_DB_NAME) -> str:
    """Return the pinned DB path, creating/updating the pin file as needed."""
    pinned = _read_pin()
    if pinned:
        try:
            pp = Path(pinned)
            # If file exists OR parent directory exists, we accept it.
            if pp.exists() or pp.parent.exists():
                return str(pp)
        except Exception:
            pass

    # Prefer project-root DB if it exists
    root_db = (PROJECT_ROOT / db_name)
    try:
        if root_db.exists():
            _write_pin(str(root_db.resolve()))
            return str(root_db.resolve())
    except Exception:
        pass

    # Otherwise, if there's an existing DB elsewhere (cwd/dashboard/parent), pin to it
    found = find_db_files(db_name=db_name)
    if found:
        _write_pin(found[0])
        return found[0]

    # Default: project root DB (will be created)
    _write_pin(str(root_db.resolve()))
    return str(root_db.resolve())


def describe_db_state(db_name: str = DEFAULT_DB_NAME) -> dict:
    """Small helper for diagnostics/doctor."""
    try:
        pinned = _read_pin()
        found = find_db_files(db_name=db_name)
        return {
            "project_root": str(PROJECT_ROOT),
            "pin_file": str(PIN_FILE),
            "pinned": pinned or "",
            "found": found,
            "duplicates": [p for p in found if pinned and p != pinned],
        }
    except Exception:
        return {
            "project_root": str(PROJECT_ROOT),
            "pin_file": str(PIN_FILE),
            "pinned": "",
            "found": [],
            "duplicates": [],
        }
