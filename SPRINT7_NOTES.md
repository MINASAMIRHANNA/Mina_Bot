# Sprint 7 (Command Center + Stable Command Queue)

## What changed

### 1) Dashboard: Command Center (Doctor page)
- Added a **Command Center** card on `/doctor`:
  - Queue common commands (RELOAD_CONFIG / CLOSE_ALL_POSITIONS / CLOSE_TRADE / EXECUTE_SIGNAL)
  - View recent commands with status, claimed_by, and last_error
  - **Cancel** a pending command
  - **Retry** a command (creates a new command row and cancels the old one)

### 2) Dashboard API: Commands endpoints
New authenticated endpoints:
- `GET /api/commands?limit=40&status=PENDING`
- `POST /api/commands/queue`  body: `{ "cmd": "RELOAD_CONFIG", "params": { ... } }`
- `POST /api/commands/{id}/cancel`
- `POST /api/commands/{id}/retry`

### 3) Status page: send command no longer depends on project_doctor
- `/status` "Send" button now calls `POST /api/commands/queue` directly.
  - This avoids schema mismatch issues inside `project_doctor.py`.

### 4) project_doctor.py: safer DB path resolution
- `resolve_db_path()` now prefers the pinned `.db_path` file (same as runtime_diag) then `cfg.DB_FILE`.

## Quick test
1) Start dashboard and login.
2) Open `/doctor` → Command Center.
3) Click **Queue** with `RELOAD_CONFIG`.
4) Verify DB:
   ```bash
   sqlite3 bot_data.db "SELECT id, cmd, status, created_at, source, claimed_by, last_error FROM commands ORDER BY id DESC LIMIT 10;"
   ```

## Notes
- All changes are additive and avoid breaking existing features.
- Commands are still processed by your existing execution monitor logic.
