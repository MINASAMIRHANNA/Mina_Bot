# Sprint 8 - Step 2 (FSM + Trade Events)

## What changed
- Added `trade_events` table (immutable history of trade lifecycle events).
- Added safe-add columns to `trades`:
  - `fsm_state`, `fsm_state_since_ms`, `fsm_state_updated_ms`
  - `protection_status`, `protection_last_check_ms`, `protection_details`, `protection_error` (used in Step 3)
- Added `ack_meta` column to `commands` (used in later Sprint 8 steps).
- Implemented helpers in `database.py`:
  - `add_trade_event(...)`
  - `set_trade_fsm_state(...)`
- Wired these into:
  - `add_trade(...)`  -> logs `TRADE_CREATED` + sets FSM to `OPEN`
  - `close_trade(...)` -> sets FSM `CLOSING` then logs `TRADE_CLOSED` + sets FSM to `CLOSED`

## Quick verification
After running the bot and opening/closing trades:

```bash
sqlite3 bot_data.db "SELECT id, symbol, status, fsm_state, fsm_state_updated_ms FROM trades ORDER BY id DESC LIMIT 10;"
sqlite3 bot_data.db "SELECT id, trade_id, event_type, event_ts, payload_json FROM trade_events ORDER BY id DESC LIMIT 20;"
```

Expected:
- New trades have `fsm_state=OPEN`.
- Closed trades end with `fsm_state=CLOSED`.
- `trade_events` contains `TRADE_CREATED`, `FSM_STATE`, `TRADE_CLOSED`.
