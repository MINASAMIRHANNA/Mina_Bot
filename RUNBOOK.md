# Mina_Bot Runbook (Sprint 4)

هدف Sprint 4: تشغيل/إيقاف المنظومة "زرار واحد" + تشخيص سريع + Restart آمن **بدون تغيير منطق التداول**.

## المكوّنات
- **Dashboard**: `uvicorn dashboard.app:app` (Port 8000)
- **Execution Monitor**: `python execution_monitor.py`
- **Main Bot**: `python main.py`

> الترتيب الموصى به: Dashboard -> Monitor -> Bot

## متطلبات
- شغّل كل شيء من **Root** (نفس فولدر `main.py`).
- يفضل وجود VirtualEnv في `.venv/` (اختياري). السكريبتات هتحاول تفعّله لو موجود.

## تشغيل زرار واحد
من Root:

```bash
bash ops/run_stack.sh
```

- هينشئ:
  - `logs/runbook/` (Logs لكل خدمة)
  - `.pids/` (PID files لكل خدمة)

## Status

```bash
bash ops/status_stack.sh
```

## إيقاف

```bash
bash ops/stop_stack.sh
```

## Restart آمن

```bash
bash ops/restart_stack.sh
```

## تشخيص سريع

```bash
python tools/runtime_diag.py
```

ولو تحب Snapshot (ملف نصي بتاريخ/وقت):

```bash
bash ops/snapshot_diag.sh
```

هينتج ملف داخل `logs/runbook/diag_YYYYMMDD_HHMMSS.txt`.

## نصائح تشغيل آمنة
- للتجربة: خلي `mode=TEST` و `PAPER_TRADING=True`.
- لو لاحظت البوت مش بيفتح صفقات جديدة: راجع `open_trades` مقابل `max_concurrent_trades`.
- لو `/api/settings` بيرجع 401 في الـ diag: ده طبيعي لو الداشبورد متقفل بـ Login.

