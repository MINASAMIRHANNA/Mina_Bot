# Sprint 8 — Step 3 (Option B)

## What was added

### 1) Per-trade protection status tracking (in DB)
Execution Monitor now **writes a per-trade protection status** into the `trades` table:
- `protection_status`
- `protection_last_check_ms`
- `protection_details` (JSON)
- `protection_error`

It updates these fields for **OPEN futures trades** on every protection audit cycle.

Status rules (high-level):
- `OK` → expected protection exists
- `MISSING_SL`, `MISSING_TP`, `MISSING_TRAIL`, or combined like `MISSING_SL_TP`
- `NO_POSITION` → DB says trade OPEN but exchange has no position for that symbol
- `SKIPPED_NO_KEYS` → no BINANCE_API_KEY/SECRET so we skipped signed calls

### 2) Dashboard buttons now actually trigger the monitor
The existing Doctor buttons:
- `Run audit now` → POST `/api/protection/force`
- `Clean orphan orders` → POST `/api/protection/orphans/cleanup`

Now **Execution Monitor watches**:
- DB setting `protection_audit_force`
- DB setting `orphan_orders_cleanup_force`

…and runs immediately when they change.

### 3) Optional orphan cleanup
When cleanup is forced, Execution Monitor cancels ALL open orders for symbols detected as:
- no exchange position
- no OPEN DB trade
- BUT protective orders exist

(Only when explicitly forced from dashboard.)

## Quick verification

### A) See the latest audit JSON (Doctor page already shows it)
- Open `/doctor`

### B) Check per-trade status directly from SQLite
```sql
SELECT id, symbol, status, protection_status, protection_last_check_ms
FROM trades
WHERE status='OPEN'
ORDER BY id DESC
LIMIT 20;
```

### C) See details JSON for a trade
```sql
SELECT id, symbol, protection_status, protection_details
FROM trades
WHERE id=55;
```

## Notes
- Option B means we attempt a real futures verification (positions + open orders). This requires BINANCE_API_KEY/SECRET.
- If keys are missing, we will not spam errors; we set `SKIPPED_NO_KEYS` so the UI is clear.
