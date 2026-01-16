// ===========================================
// 1. WEBSOCKET & GLOBALS
// ===========================================
const wsProtocol = window.location.protocol === "https:" ? "wss" : "ws";
const ws = new WebSocket(`${wsProtocol}://${window.location.host}/ws/signals`);

// Chart Instances
let equityChartInst = null;
let dailyChartInst = null;
let dirChartInst = null;

// ===========================================
// 2. WS HANDLERS
// ===========================================
const statusText = document.getElementById('status-text');

ws.onopen = () => { 
    if(statusText) { statusText.innerText = "System Online"; statusText.style.color = "#2ea043"; }
};

ws.onclose = () => { 
    if(statusText) { statusText.innerText = "Disconnected"; statusText.style.color = "#da3633"; }
};

ws.onmessage = (event) => {
    const data = JSON.parse(event.data);
    addSignalCard(data);

    const coin = data.symbol || data.coin || data.pair || 'N/A';
    const sig = data.signal || data.side || data.direction || 'SIGNAL';
    const status = data.status || data.state || '';
    const conf = (data.confidence !== undefined ? data.confidence : (data.score !== undefined ? data.score : 'N/A'));

    const statusTxt = status ? ` • ${status}` : '';
    logToTerminal(`SIGNAL: <b>${coin}</b> ${sig} (Conf: ${conf})${statusTxt}`, 'SIGNAL');
};

function logToTerminal(msg, level='INFO') {
    const logBox = document.getElementById('console-logs');
    if (logBox) {
        const time = new Date().toLocaleTimeString();
        const logEntry = document.createElement('div');
        logEntry.className = 'log-line';
        logEntry.innerHTML = `
            <span style="color:#484f58; margin-right:8px;">${time}</span>
            <span class="lvl-${level}">${msg}</span>
        `;
        logBox.prepend(logEntry); 
        if (logBox.children.length > 50) logBox.lastChild.remove(); 
    }
}

// ===========================================
// 3. UI HELPERS (SIGNALS & POSITIONS)
// ===========================================
function addSignalCard(data, opts = {}) {
    const container = document.getElementById('signals');
    if (!container) return;

    const n = normalizeSignalPayload(data);
    const id = n.id;

    let card = null;
    if (id !== null && id !== undefined && id !== '') {
        card = container.querySelector(`[data-signal-id="${id}"]`);
    }

    const isNew = !card;
    if (!card) {
        card = document.createElement('div');
        if (id !== null && id !== undefined && id !== '') card.dataset.signalId = String(id);
    }

    card.className = `card ${n.isLong ? 'bullish' : 'bearish'}`;

    const statusBadge = n.status
        ? `<span class="badge" style="border:1px solid #30363d; background:#0d1117; margin-left:6px;">${escapeHtml(n.status)}</span>`
        : '';

    const actions = (id && n.status === 'PENDING_APPROVAL')
        ? `<div style="margin-top:10px; display:flex; gap:8px;">
                <button class="btn btn-primary" onclick="approveSignal(${id})">Approve</button>
                <button class="btn btn-secondary" onclick="rejectSignal(${id})">Reject</button>
           </div>`
        : '';

    const timeTxt = n.ts ? new Date(n.ts).toLocaleTimeString() : new Date().toLocaleTimeString();

    card.innerHTML = `
        <div class="card-header">
            <span class="card-symbol">${escapeHtml(n.coin)}</span>
            <span class="badge">${escapeHtml(n.signal)}</span>
            ${statusBadge}
        </div>

        <div style="display:grid; grid-template-columns:1fr 1fr; gap:5px; font-size:0.85rem; color:#848d97;">
            <div>Entry: <b style="color:#e6edf3">${Number(n.entry || 0).toFixed(6)}</b></div>
            <div>Score: <b style="color:#e6edf3">${Number(n.score || 0).toFixed(2)}</b></div>
            <div>Market: <b>${escapeHtml((n.market_type || '—').toUpperCase())}</b></div>
            <div>TF: <b>${escapeHtml(n.timeframe || '—')}</b></div>
        </div>

        ${n.reason ? `<div style="margin-top:8px; font-size:0.8rem; color:#8b949e;">${escapeHtml(n.reason)}</div>` : ''}

        ${actions}

        <div style="margin-top:8px; font-size:0.7rem; color:#64748b; text-align:right;">${timeTxt}</div>
    `;

    if (isNew) {
        if (opts.append) container.appendChild(card);
        else container.prepend(card);
    }

    // Keep UI light
    while (container.children.length > 50) container.lastChild.remove();
}

function normalizeSignalPayload(data) {
    const coin = data.symbol || data.coin || data.pair || 'N/A';

    let sig = (data.signal || data.direction || data.side || '').toString().trim();
    sig = sig.toUpperCase();
    if (sig === 'BUY') sig = 'LONG';
    if (sig === 'SELL') sig = 'SHORT';
    if (!sig) sig = 'SIGNAL';

    const status = (data.status || data.state || '').toString().trim().toUpperCase() || 'NEW';

    const explain = data.explain || (data.meta && data.meta.explain) || {};
    const entry = data.entry_price || (data.entry && (data.entry.low || data.entry)) || data.entry || 0;
    const score = (data.confidence !== undefined ? data.confidence : (data.score !== undefined ? data.score : 0));

    const id = (data.id !== undefined ? data.id :
              (data.signal_id !== undefined ? data.signal_id :
              (data.inbox_id !== undefined ? data.inbox_id : '')));

    const ts = data.timestamp || data.created_at || data.time || data.ts || null;

    return {
        id,
        coin,
        signal: sig,
        isLong: String(sig).includes('LONG'),
        status,
        explain,
        entry,
        score,
        reason: data.reason || (data.meta && data.meta.reason) || '',
        market_type: data.market_type || data.market || '',
        timeframe: data.timeframe || data.interval || '',
        ts,
    };
}

async function loadSignalInbox(opts = {}) {
    const status = (opts.status !== undefined ? opts.status : 'PENDING_APPROVAL');
    const limit = (opts.limit !== undefined ? opts.limit : 30);

    const container = document.getElementById('signals');
    if (!container) return;

    try {
        const url = status
            ? `/api/signals?status=${encodeURIComponent(status)}&limit=${encodeURIComponent(limit)}`
            : `/api/signals?limit=${encodeURIComponent(limit)}`;

        const res = await fetch(url);
        if (!res.ok) {
            const txt = await res.text().catch(() => "");
            container.innerHTML = `<div class="panel" style="padding:12px;">⚠️ Failed to load signals (HTTP ${res.status}). ${txt ? "Response: " + txt.slice(0,200) : ""}</div>`;
            return;
        }

        let j;
        try {
            j = await res.json();
        } catch (e) {
            container.innerHTML = `<div class="panel" style="padding:12px;">⚠️ Could not parse response. Refresh and try again.</div>`;
            return;
        }

        // Normalize list payload safely
        let items = [];
        if (Array.isArray(j)) items = j;
        else if (j && Array.isArray(j.items)) items = j.items;
        else if (j && Array.isArray(j.signals)) items = j.signals;
        else if (j && Array.isArray(j.data)) items = j.data;
        else if (j && Array.isArray(j.results)) items = j.results;
        else if (j && (j.msg || j.detail || j.error)) {
            const msg = j.msg || j.detail || j.error;
            container.innerHTML = `<div class="panel" style="padding:12px;">⚠️ ${msg}</div>`;
            return;
        }

        container.innerHTML = '';
        if (!items || items.length === 0) {
            container.innerHTML = `<div class="panel" style="padding:12px;">No signals yet. Send a test signal to <code>/publish</code> or run the bot.</div>`;
            return;
        }
        items.forEach(it => addSignalCard(it, { append: true }));
    } catch (e) {
        console.error("loadSignalInbox error:", e);
    }
}

async function approveSignal(id) {
    if (!confirm(`Approve signal #${id}? This will enqueue EXECUTE_SIGNAL for the bot.`)) return;
    const note = prompt("Note (optional):", "") || "";
    try {
        const res = await fetch('/api/signals/approve', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ id: id, note: note })
        });
        const j = await res.json().catch(() => ({}));
        if (!res.ok) {
            alert("❌ Approve failed: " + (j.detail || j.msg || res.statusText));
            return;
        }
        logToTerminal(`APPROVED signal #${id}`, 'INFO');
        await loadSignalInbox({ status: '' , limit: 30 });
    } catch (e) {
        console.error(e);
        alert("⚠️ Connection error approving signal.");
    }
}

async function rejectSignal(id) {
    const reason = prompt("Reject reason:", "Not suitable") || "Not suitable";
    const note = prompt("Note (optional):", "") || "";
    if (!confirm(`Reject signal #${id}?`)) return;

    try {
        const res = await fetch('/api/signals/reject', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ id: id, reason: reason, note: note })
        });
        const j = await res.json().catch(() => ({}));
        if (!res.ok) {
            alert("❌ Reject failed: " + (j.detail || j.msg || res.statusText));
            return;
        }
        logToTerminal(`REJECTED signal #${id} (${escapeHtml(reason)})`, 'INFO');
        await loadSignalInbox({ status: '' , limit: 30 });
    } catch (e) {
        console.error(e);
        alert("⚠️ Connection error rejecting signal.");
    }
}


async function fetchPositions() {
    const res = await fetch('/api/positions');
    const trades = await res.json();
    const container = document.getElementById('positions-grid');
    const openPnlEl = document.getElementById('open-pnl-value');

    if(!container || !openPnlEl) return;

    if(!Array.isArray(trades) || trades.length === 0) {
        container.innerHTML = '<div style="grid-column:1/-1; text-align:center; padding:30px; color:#848d97; border:1px dashed #30363d;">No active positions</div>';
        openPnlEl.innerText = "$0.00";
        openPnlEl.style.color = "#e6edf3";
        return;
    }

    container.innerHTML = '';
    let totalOpenPnl = 0;

    for (const t of trades) {
        const pnlRaw = (t && (t.unrealized_pnl ?? t.pnl)) ?? 0;
        const pnlNum = Number(pnlRaw);
        const pnl = Number.isFinite(pnlNum) ? pnlNum : 0;
        totalOpenPnl += pnl;

        const sigU = String(t.signal || "").toUpperCase();
        const dirU = String(t.direction || "").toUpperCase();
        const direction = dirU || ((sigU.includes("SHORT") || sigU.includes("SELL")) ? "SHORT" : ((sigU.includes("LONG") || sigU.includes("BUY")) ? "LONG" : "UNKNOWN"));
        const isLong = (direction === "LONG");

        const entry = Number(t.entry_price);
        const qty = Number(t.quantity);

        const div = document.createElement('div');
        div.className = `card ${isLong ? 'bullish' : 'bearish'}`;
        const verifiedIcon = t.verified ? '<i class="fa-solid fa-circle-check" style="color:var(--ok); margin-left:5px;" title="Verified Binance Data"></i>' : '';

        div.innerHTML = `
            <div class="card-header">
                <span class="card-symbol">${escapeHtml(t.symbol || "")} ${verifiedIcon}</span>
                <span class="badge">${escapeHtml(direction)}</span>
            </div>
            <div style="display:grid; grid-template-columns:1fr 1fr; gap:5px; font-size:0.85rem; color:#848d97;">
                <div>Signal: <b style="color:#e6edf3">${escapeHtml(t.signal || direction)}</b></div>
                <div>Market: <b style="color:#e6edf3">${escapeHtml(t.market_type || "")}</b></div>
                <div>Entry: <b style="color:#e6edf3">${Number.isFinite(entry) ? entry.toFixed(9) : "—"}</b></div>
                <div>Size: <b style="color:#e6edf3">${Number.isFinite(qty) ? qty.toFixed(3) : "—"}</b></div>
                <div>PnL: <b style="color:${pnl >= 0 ? '#2ea043' : '#da3633'}">$${pnl.toFixed(2)}</b></div>
            </div>
            <button class="btn-close" onclick="closePosition('${t.symbol}')">CLOSE TRADE</button>
        `;
        container.appendChild(div);
    }

    openPnlEl.innerText = `$${totalOpenPnl.toFixed(2)}`;
    openPnlEl.style.color = totalOpenPnl >= 0 ? '#2ea043' : '#da3633';
}

// ===========================================
// 4. LOGS & STATS
// ===========================================
async function fetchLogs() {
    const res = await fetch('/api/logs');
    const logs = await res.json();
    const logBox = document.getElementById('console-logs');
    if(logBox) {
        // Only update if content changed slightly to avoid flicker (simplified here)
        logBox.innerHTML = logs.map(l => `<div class="log-line"><span style="color:#484f58; margin-right:8px;">${l.time.split('T')[1].split('.')[0]}</span><span class="lvl-${l.level}">${l.msg}</span></div>`).join('');
    }
}

async function fetchStats() {
    const res = await fetch('/api/stats');
    const s = await res.json();
    const pnlEl = document.getElementById('pnl-value');
    if(pnlEl) {
        pnlEl.innerText = `$${s.pnl}`;
        pnlEl.style.color = s.pnl >= 0 ? '#2ea043' : '#da3633';
    }
    const winEl = document.getElementById('win-rate');
    if(winEl) winEl.innerText = `${s.win_rate}%`;
}

async function fetchAdvancedStats() {
    try {
        const res = await fetch('/api/advanced_stats');
        const d = await res.json();
        
        const pf = document.getElementById('profit-factor');
        if(pf) pf.innerText = d.profit_factor;
        
        const dd = document.getElementById('max-drawdown');
        if(dd) {
            dd.innerText = d.max_drawdown + "%";
            dd.style.color = "#da3633";
        }
        
        const fee = document.getElementById('fees-total');
        if(fee) fee.innerText = "$" + d.total_fees;
    } catch(e) { console.error("Stats Error:", e); }
}

// ===========================================
// 5. CHARTS & REPORTS
// ===========================================
async function loadEquityChart() {
    const canvas = document.getElementById('equityChart');
    if(!canvas) return;
    
    const res = await fetch('/api/equity_chart');
    const data = await res.json();
    const ctx = canvas.getContext('2d');
    
    if(equityChartInst) equityChartInst.destroy();
    
    equityChartInst = new Chart(ctx, {
        type: 'line',
        data: {
            labels: data.labels,
            datasets: [{
                label: 'Account Equity ($)',
                data: data.equity,
                borderColor: '#1f6feb',
                backgroundColor: 'rgba(31, 111, 235, 0.1)',
                borderWidth: 2,
                fill: true,
                tension: 0.3,
                pointRadius: 0
            }]
        },
        options: {
            responsive: true, maintainAspectRatio: false,
            plugins: { legend: { display: false } },
            scales: { x: { grid: { display: false } }, y: { grid: { color: '#30363d' } } }
        }
    });
}

async function loadFullReport() {
    const res = await fetch('/api/full_report');
    const data = await res.json();
    
    // Metrics
    const pnlEl = document.getElementById('rep-net-pnl');
    pnlEl.innerText = `$${data.metrics.net_pnl}`;
    pnlEl.style.color = data.metrics.net_pnl >= 0 ? '#2ea043' : '#da3633';
    document.getElementById('rep-win-rate').innerText = `${data.metrics.win_rate}%`;
    document.getElementById('rep-total-trades').innerText = data.metrics.total_trades;

    // Daily Chart
    const ctx1 = document.getElementById('dailyPnlChart').getContext('2d');
    if (dailyChartInst) dailyChartInst.destroy();
    dailyChartInst = new Chart(ctx1, {
        type: 'bar',
        data: {
            labels: data.daily.map(d => d.day),
            datasets: [{
                label: 'Daily PnL ($)',
                data: data.daily.map(d => d.daily_pnl),
                backgroundColor: data.daily.map(d => d.daily_pnl >= 0 ? '#2ea043' : '#da3633'),
                borderRadius: 4
            }]
        },
        options: {
            responsive: true, maintainAspectRatio: false,
            plugins: { legend: { display: false } },
            scales: { 
                y: { grid: { color: '#30363d' }, ticks: { color: '#8b949e' } },
                x: { grid: { display: false }, ticks: { color: '#8b949e' } }
            }
        }
    });

    // Direction Chart
    const ctx2 = document.getElementById('directionChart').getContext('2d');
    if (dirChartInst) dirChartInst.destroy();
    const longs = data.directions.find(d => d.direction === 'LONG');
    const shorts = data.directions.find(d => d.direction === 'SHORT');
    dirChartInst = new Chart(ctx2, {
        type: 'doughnut',
        data: {
            labels: ['Long PnL', 'Short PnL'],
            datasets: [{
                data: [longs ? longs.total_pnl : 0, shorts ? shorts.total_pnl : 0],
                backgroundColor: ['#2ea043', '#da3633'],
                borderWidth: 0
            }]
        },
        options: {
            responsive: true, maintainAspectRatio: false,
            plugins: { legend: { position: 'bottom', labels: { color: '#e6edf3' } } }
        }
    });

    // Tables
    const coins = data.coins;
    const bestCoins = coins.slice(0, 5); 
    const worstCoins = coins.slice().sort((a,b) => a.total_pnl - b.total_pnl).slice(0, 5);
    
    fillCoinTable('top-coins-body', bestCoins);
    fillCoinTable('worst-coins-body', worstCoins);
}

function fillCoinTable(elementId, coinList) {
    const tbody = document.getElementById(elementId);
    tbody.innerHTML = '';
    
    coinList.forEach(c => {
        const winRate = (c.wins / c.count * 100).toFixed(1);
        const tr = document.createElement('tr');
        tr.style.borderBottom = '1px solid #21262d';
        tr.innerHTML = `
            <td style="padding:8px; font-weight:bold;">${c.symbol}</td>
            <td style="text-align:center;">${c.count}</td>
            <td style="text-align:center; color:${winRate >= 50 ? '#2ea043' : '#da3633'}">${winRate}%</td>
            <td style="text-align:center; font-weight:bold; color:${c.total_pnl >= 0 ? '#2ea043' : '#da3633'}">
                $${c.total_pnl.toFixed(2)}
            </td>
        `;
        tbody.appendChild(tr);
    });
    if (coinList.length === 0) tbody.innerHTML = '<tr><td colspan="4" style="text-align:center; padding:10px; color:#8b949e;">No Data</td></tr>';
}

// ===========================================
// 6. SETTINGS & CONFIG
// ===========================================
async function loadSettings() {
    const res = await fetch('/api/settings');
    const data = await res.json();
    if(document.activeElement.tagName !== 'INPUT') {
        document.getElementById('mode-select').value = data.mode;
        document.getElementById('scalp-size').value = data.scalp_size_usd;
        document.getElementById('swing-size').value = data.swing_size_usd;
        document.getElementById('max-trade-usd').value = data.max_trade_usd;
        document.getElementById('max-concurrent-trades').value = data.max_concurrent_trades || 30;
        document.getElementById('strategy-mode').value = data.strategy_mode || "CONSERVATIVE";
        document.getElementById('blacklist-input').value = data.blacklist || "";
    }
}

async function updateSettings() {
    const payload = {
        mode: document.getElementById('mode-select').value,
        scalp_size_usd: document.getElementById('scalp-size').value,
        swing_size_usd: document.getElementById('swing-size').value,
        max_trade_usd: document.getElementById('max-trade-usd').value,
        max_concurrent_trades: document.getElementById('max-concurrent-trades').value,
        strategy_mode: document.getElementById('strategy-mode').value,
        blacklist: document.getElementById('blacklist-input').value
    };
    await fetch('/api/settings', { 
        method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify(payload) 
    });
}


// -------------------------------------------
// 6A. CORE TRADING SETTINGS (AI Config tab)
// -------------------------------------------
function _setVal(id, val, fallback = "") {
    const el = document.getElementById(id);
    if (!el) return;
    if (val === undefined || val === null || String(val).trim() === "") el.value = fallback;
    else el.value = String(val);
}

function _setSel(id, val, fallback = null) {
    const el = document.getElementById(id);
    if (!el) return;
    const v = (val === undefined || val === null) ? "" : String(val);
    if (v !== "") el.value = v;
    else if (fallback !== null) el.value = fallback;
}

async function loadCoreTradingSettings() {
    try {
        const res = await fetch('/api/settings');
        const data = await res.json();

        _setVal('ai-core-scalp-size', data.scalp_size_usd, "");
        _setVal('ai-core-swing-size', data.swing_size_usd, "");
        _setVal('ai-core-max-trade-usd', data.max_trade_usd, "");
        _setVal('ai-core-max-trades', data.max_concurrent_trades, "");

        _setVal('ai-core-lev-scalp', data.leverage_scalp, "");
        _setVal('ai-core-lev-swing', data.leverage_swing, "");
        _setVal('ai-core-max-lev', data.max_leverage, "");

        _setVal('ai-core-entry-threshold', data.entry_threshold, "");
        _setVal('ai-core-trail-trigger', data.trail_trigger, "");
        _setVal('ai-core-trail-trigger-roi', data.trail_trigger_roi, "");

        _setSel('ai-core-require-approval', data.require_dashboard_approval, "TRUE");
        _setSel('ai-core-auto-live', data.auto_approve_live, "FALSE");
        _setSel('ai-core-auto-paper', data.auto_approve_paper, "TRUE");
        _setSel('ai-core-block-open', data.block_if_open_position, "FALSE");

        _setVal('ai-core-cd-trade', data.cooldown_after_trade_sec, "");
        _setVal('ai-core-cd-sl', data.cooldown_after_sl_sec, "");
        _setVal('ai-core-dedupe', data.inbox_dedupe_sec, "");
        _setVal('ai-core-lock-ttl', data.symbol_lock_ttl_sec, "");

        _setSel('ai-core-hard-tp', data.hard_tp_enabled, "FALSE");
        _setSel('ai-core-ensure-protect', data.ensure_protection_orders, "TRUE");
        _setVal('ai-core-ensure-sec', data.ensure_protection_every_sec, "");

        _setSel('ai-core-strategy-mode', data.strategy_mode, "CONSERVATIVE");
        _setVal('ai-core-blacklist', data.blacklist, "");

    } catch (e) {
        console.error("loadCoreTradingSettings failed:", e);
    }
}

async function reloadCoreTradingSettings() {
    await loadCoreTradingSettings();
    alert("✅ Core Trading settings reloaded.");
}

async function saveCoreTradingSettings() {
    if (!confirm("Save Core Trading settings to DB?\n\nThis affects main.py and will apply on next config reload.")) return;

    const payload = {
        scalp_size_usd: document.getElementById('ai-core-scalp-size')?.value,
        swing_size_usd: document.getElementById('ai-core-swing-size')?.value,
        max_trade_usd: document.getElementById('ai-core-max-trade-usd')?.value,
        max_concurrent_trades: document.getElementById('ai-core-max-trades')?.value,

        leverage_scalp: document.getElementById('ai-core-lev-scalp')?.value,
        leverage_swing: document.getElementById('ai-core-lev-swing')?.value,
        max_leverage: document.getElementById('ai-core-max-lev')?.value,

        entry_threshold: document.getElementById('ai-core-entry-threshold')?.value,
        trail_trigger: document.getElementById('ai-core-trail-trigger')?.value,
        trail_trigger_roi: document.getElementById('ai-core-trail-trigger-roi')?.value,

        require_dashboard_approval: document.getElementById('ai-core-require-approval')?.value,
        auto_approve_live: document.getElementById('ai-core-auto-live')?.value,
        auto_approve_paper: document.getElementById('ai-core-auto-paper')?.value,
        block_if_open_position: document.getElementById('ai-core-block-open')?.value,

        cooldown_after_trade_sec: document.getElementById('ai-core-cd-trade')?.value,
        cooldown_after_sl_sec: document.getElementById('ai-core-cd-sl')?.value,
        inbox_dedupe_sec: document.getElementById('ai-core-dedupe')?.value,
        symbol_lock_ttl_sec: document.getElementById('ai-core-lock-ttl')?.value,

        hard_tp_enabled: document.getElementById('ai-core-hard-tp')?.value,
        ensure_protection_orders: document.getElementById('ai-core-ensure-protect')?.value,
        ensure_protection_every_sec: document.getElementById('ai-core-ensure-sec')?.value,

        strategy_mode: document.getElementById('ai-core-strategy-mode')?.value,
        blacklist: document.getElementById('ai-core-blacklist')?.value
    };

    // Drop undefined keys
    Object.keys(payload).forEach(k => {
        if (payload[k] === undefined) delete payload[k];
    });

    try {
        const res = await fetch('/api/settings', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify(payload)
        });
        const data = await res.json();
        if (data.status === 'ok') {
            alert("✅ Core Trading settings saved.");
            // refresh sidebar quick controls too (if user still uses them)
            loadSettings();
        } else {
            alert("Save failed: " + (data.detail || data.error || "Unknown error"));
        }
    } catch (e) {
        console.error(e);
        alert("Save failed. Check console / server logs for details.");
    }
}

async function sendReloadModels() {
    if(!confirm("Send RELOAD_MODELS command now?")) return;
    try {
        await fetch('/api/command', {
            method:'POST',
            headers:{'Content-Type':'application/json'},
            body: JSON.stringify({cmd:'RELOAD_MODELS', params:{reason:'DASHBOARD_ACTION'}})
        });
        alert("✅ Reload models command sent.");
    } catch(e) {
        console.error(e);
        alert("Failed to send command.");
    }
}


// AI Config
async function loadAdvancedConfig() {
    const res = await fetch('/api/settings');
    const data = await res.json();

    const setVal = (id, fallback) => {
        const el = document.getElementById(id);
        if (!el) return;
        const v = data && Object.prototype.hasOwnProperty.call(data, id) ? data[id] : undefined;
        // We map ids to keys below; here fallback is used when value is null/undefined/empty string.
        const val = (fallback !== undefined) ? fallback : "";
        el.value = (v !== undefined && v !== null && String(v).trim() !== "") ? v : val;
    };

    // Gates / Filters
    const elAtr = document.getElementById('cfg-atr'); if (elAtr) elAtr.value = (data.gate_atr_min_pct !== undefined && String(data.gate_atr_min_pct).trim() !== "") ? data.gate_atr_min_pct : 0.003;
    const elAdx = document.getElementById('cfg-adx'); if (elAdx) elAdx.value = (data.gate_adx_min !== undefined && String(data.gate_adx_min).trim() !== "") ? data.gate_adx_min : 10;
    const elRsi = document.getElementById('cfg-rsi'); if (elRsi) elRsi.value = (data.rsi_max_buy !== undefined && String(data.rsi_max_buy).trim() !== "") ? data.rsi_max_buy : 70;
    const elPump = document.getElementById('cfg-pump'); if (elPump) elPump.value = (data.pump_score_min !== undefined && String(data.pump_score_min).trim() !== "") ? data.pump_score_min : 75;

    // Paper Tournament
    const elPE = document.getElementById('cfg-paper-enabled'); if (elPE) elPE.value = (data.paper_tournament_enabled || "FALSE");
    const elPD = document.getElementById('cfg-paper-daily'); if (elPD) elPD.value = (data.paper_daily_enabled || "TRUE");
    const elPT = document.getElementById('cfg-paper-time'); if (elPT) elPT.value = (data.paper_daily_time_utc || "00:05");


// Exchange Trailing Stop
const elXT = document.getElementById('cfg-extrail-enabled'); if (elXT) elXT.value = (data.exchange_trailing_enabled || "FALSE");
const elXA = document.getElementById('cfg-extrail-activate'); if (elXA) elXA.value = (data.exchange_trailing_activation_roi !== undefined && data.exchange_trailing_activation_roi !== null && String(data.exchange_trailing_activation_roi).trim() !== "") ? data.exchange_trailing_activation_roi : 0.01;
const elXC = document.getElementById('cfg-extrail-callback'); if (elXC) elXC.value = (data.exchange_trailing_callback_rate !== undefined && data.exchange_trailing_callback_rate !== null && String(data.exchange_trailing_callback_rate).trim() !== "") ? data.exchange_trailing_callback_rate : 0.3;
const elXW = document.getElementById('cfg-extrail-working'); if (elXW) elXW.value = (data.exchange_trailing_working_type || "MARK_PRICE");

    // AI Confidence Thresholds
    const elMinScalp = document.getElementById('cfg-min-scalp'); if (elMinScalp) elMinScalp.value = (data.min_conf_scalp !== undefined && String(data.min_conf_scalp).trim() !== "") ? data.min_conf_scalp : 55;
    const elMinSwing = document.getElementById('cfg-min-swing'); if (elMinSwing) elMinSwing.value = (data.min_conf_swing !== undefined && String(data.min_conf_swing).trim() !== "") ? data.min_conf_swing : 65;

    // Risk & Reward Multipliers (ATR)
    const elSlScalp = document.getElementById('cfg-sl-scalp'); if (elSlScalp) elSlScalp.value = (data.sl_scalp_mult !== undefined && String(data.sl_scalp_mult).trim() !== "") ? data.sl_scalp_mult : 0.8;
    const elTpScalp = document.getElementById('cfg-tp-scalp'); if (elTpScalp) elTpScalp.value = (data.tp_scalp_mult !== undefined && String(data.tp_scalp_mult).trim() !== "") ? data.tp_scalp_mult : 1.2;
    const elSlSwing = document.getElementById('cfg-sl-swing'); if (elSlSwing) elSlSwing.value = (data.sl_swing_mult !== undefined && String(data.sl_swing_mult).trim() !== "") ? data.sl_swing_mult : 1.5;
    const elTpSwing = document.getElementById('cfg-tp-swing'); if (elTpSwing) elTpSwing.value = (data.tp_swing_mult !== undefined && String(data.tp_swing_mult).trim() !== "") ? data.tp_swing_mult : 2.0;

    // Technical Indicators Parameters
    const elRsiLen = document.getElementById('cfg-rsi-len'); if (elRsiLen) elRsiLen.value = (data.ind_rsi_len !== undefined && String(data.ind_rsi_len).trim() !== "") ? data.ind_rsi_len : 14;
    const elAdxLen = document.getElementById('cfg-adx-len'); if (elAdxLen) elAdxLen.value = (data.ind_adx_len !== undefined && String(data.ind_adx_len).trim() !== "") ? data.ind_adx_len : 14;
    const elMaFast = document.getElementById('cfg-ma-fast'); if (elMaFast) elMaFast.value = (data.ind_ma_fast !== undefined && String(data.ind_ma_fast).trim() !== "") ? data.ind_ma_fast : 50;
    const elMaSlow = document.getElementById('cfg-ma-slow'); if (elMaSlow) elMaSlow.value = (data.ind_ma_slow !== undefined && String(data.ind_ma_slow).trim() !== "") ? data.ind_ma_slow : 200;
}

async function saveAdvancedConfig() {
    // 1. تجميع كل القيم الجديدة من الواجهة
    const payload = {
        // فلاتر البوابة (Gates)
        "gate_atr_min_pct": document.getElementById('cfg-atr').value,
        "gate_adx_min": document.getElementById('cfg-adx').value,
        "rsi_max_buy": document.getElementById('cfg-rsi').value,
        "pump_score_min": document.getElementById('cfg-pump').value,
        "paper_tournament_enabled": document.getElementById('cfg-paper-enabled').value,
        "paper_eval_enabled": (document.getElementById('cfg-paper-enabled').value === "TRUE") ? "TRUE" : "FALSE",
        "paper_daily_enabled": document.getElementById('cfg-paper-daily').value,
        "paper_daily_time_utc": document.getElementById('cfg-paper-time').value,

        // Exchange Trailing Stop (native)
        "exchange_trailing_enabled": document.getElementById('cfg-extrail-enabled').value,
        "exchange_trailing_activation_roi": document.getElementById('cfg-extrail-activate').value,
        "exchange_trailing_callback_rate": document.getElementById('cfg-extrail-callback').value,
        "exchange_trailing_working_type": document.getElementById('cfg-extrail-working').value,


        
        // مستويات الثقة (Confidence)
        "min_conf_scalp": document.getElementById('cfg-min-scalp').value,
        "min_conf_swing": document.getElementById('cfg-min-swing').value,

        // مضاعفات المخاطرة (Risk Multipliers)
        "sl_scalp_mult": document.getElementById('cfg-sl-scalp').value,
        "tp_scalp_mult": document.getElementById('cfg-tp-scalp').value,
        "sl_swing_mult": document.getElementById('cfg-sl-swing').value,
        "tp_swing_mult": document.getElementById('cfg-tp-swing').value,

        // إعدادات المؤشرات (Indicators)
        "ind_rsi_len": document.getElementById('cfg-rsi-len').value,
        "ind_adx_len": document.getElementById('cfg-adx-len').value,
        "ind_ma_fast": document.getElementById('cfg-ma-fast').value,
        "ind_ma_slow": document.getElementById('cfg-ma-slow').value
    };

    try {
        // 2. إرسال الطلب للسيرفر
        const response = await fetch('/api/settings', { 
            method: 'POST', 
            headers: { 'Content-Type': 'application/json' }, 
            body: JSON.stringify(payload) 
        });

        if (response.ok) {
            alert("✅ AI Brain & Neural Parameters Updated!");
        } else {
            const errorData = await response.json();
            alert("❌ Update Failed: " + (errorData.detail || "Server Error"));
        }
    } catch (error) {
        console.error("Save Config Error:", error);
        alert("⚠️ Connection Error: Could not reach the server.");
    }
}
// Strategy Lab
async function loadLearningMatrix() {
    const res = await fetch('/api/learning_matrix');
    const data = await res.json();
    const tbody = document.getElementById('learning-table-body');
    tbody.innerHTML = '';
    
    data.forEach(row => {
        let adjColor = '#8b949e'; 
        let adjText = "Stable";
        if (row.ai_adj > 0) { adjColor = '#da3633'; adjText = `Stricter (+${row.ai_adj}%)`; } 
        else if (row.ai_adj < 0) { adjColor = '#2ea043'; adjText = `Looser (${row.ai_adj}%)`; }

        const tr = document.createElement('tr');
        tr.style.borderBottom = "1px solid #21262d";
        tr.innerHTML = `
            <td style="padding:10px; font-weight:bold;">${row.strategy}</td>
            <td>${row.trades}</td>
            <td style="color:${row.win_rate >= 50 ? '#2ea043' : '#da3633'}">${row.win_rate}%</td>
            <td style="color:${row.pnl >= 0 ? '#2ea043' : '#da3633'}">$${row.pnl}</td>
            <td style="color:${adjColor}; font-weight:bold;">${adjText}</td>
        `;
        tbody.appendChild(tr);
    });
    
    if (data.length === 0) tbody.innerHTML = '<tr><td colspan="5" style="text-align:center; padding:20px;">No learning data yet.</td></tr>';
}

// Secrets (DB config - Zero-ENV)
async function loadEnvSecrets() {
    try {
        const res = await fetch('/api/env_secrets');
        const data = await res.json();
        // Credentials
        if (data.BINANCE_API_KEY) document.getElementById('env-binance-key').value = data.BINANCE_API_KEY;
        if (data.BINANCE_API_SECRET) document.getElementById('env-binance-secret').value = data.BINANCE_API_SECRET;
        if (data.TELEGRAM_BOT_TOKEN) document.getElementById('env-tg-token').value = data.TELEGRAM_BOT_TOKEN;
        if (data.TELEGRAM_CHAT_ID) document.getElementById('env-tg-id').value = data.TELEGRAM_CHAT_ID;
        if (data.TELEGRAM_ERROR_CHAT_ID) document.getElementById('env-tg-err-id').value = data.TELEGRAM_ERROR_CHAT_ID;
        // System & Risk
        if (data.USE_TESTNET) document.getElementById('env-use-testnet').value = data.USE_TESTNET.toLowerCase();
        if (data.ENABLE_LIVE_TRADING) document.getElementById('env-live-trading').value = data.ENABLE_LIVE_TRADING.toLowerCase();
        if (data.RUN_MODE) document.getElementById('env-run-mode').value = data.RUN_MODE;
        if (data.HEARTBEAT_INTERVAL) document.getElementById('env-heartbeat').value = data.HEARTBEAT_INTERVAL;
        if (data.WS_CHUNK_SIZE) document.getElementById('env-chunk').value = data.WS_CHUNK_SIZE;
        if (data.TRAIL_TRIGGER) document.getElementById('env-trail').value = data.TRAIL_TRIGGER;
        if (data.DEFAULT_RISK_PCT) document.getElementById('env-risk-pct').value = data.DEFAULT_RISK_PCT;
        if (data.MAX_LEVERAGE) document.getElementById('env-max-lev').value = data.MAX_LEVERAGE;
        if (data.LEVERAGE_SCALP) document.getElementById('env-lev-scalp').value = data.LEVERAGE_SCALP;
        if (data.LEVERAGE_SWING) document.getElementById('env-lev-swing').value = data.LEVERAGE_SWING;
        if (data.SL_MULTIPLIER) document.getElementById('env-sl-mult').value = data.SL_MULTIPLIER;
        if (data.TP_MULTIPLIER) document.getElementById('env-tp-mult').value = data.TP_MULTIPLIER;
        if (data.ENTRY_THRESHOLD) document.getElementById('env-entry-thresh').value = data.ENTRY_THRESHOLD;
        if (data.WATCHLIST_VOL_LIMIT) document.getElementById('env-vol-limit').value = data.WATCHLIST_VOL_LIMIT;
        if (data.WATCHLIST_GAINERS_LIMIT) document.getElementById('env-gain-limit').value = data.WATCHLIST_GAINERS_LIMIT;
        if (data.MAX_CONCURRENT_TASKS) document.getElementById('env-max-tasks').value = data.MAX_CONCURRENT_TASKS;
        if (data.STARTUP_SCAN_LIMIT) document.getElementById('env-startup-limit').value = data.STARTUP_SCAN_LIMIT;
    
        // --- Extra DB-backed config (Zero-ENV) ---
        if (data.BINANCE_FUTURES_REST_BASE !== undefined) document.getElementById('env-fut-rest').value = data.BINANCE_FUTURES_REST_BASE;
        if (data.BINANCE_FUTURES_WS_BASE !== undefined) document.getElementById('env-fut-ws').value = data.BINANCE_FUTURES_WS_BASE;

        if (data.BOT_NAME !== undefined) document.getElementById('env-bot-name').value = data.BOT_NAME;
        if (data.DASHBOARD_URL !== undefined) document.getElementById('env-dashboard-url').value = data.DASHBOARD_URL;
        if (data.DASHBOARD_PUBLISH_URL !== undefined) document.getElementById('env-dashboard-publish-url').value = data.DASHBOARD_PUBLISH_URL;
        if (data.DASHBOARD_PUBLISH_TOKEN !== undefined) document.getElementById('env-dashboard-publish-token').value = data.DASHBOARD_PUBLISH_TOKEN;
        if (data.DASHBOARD_ENVELOPE !== undefined) document.getElementById('env-dashboard-envelope').value = String(data.DASHBOARD_ENVELOPE).toLowerCase();

        if (data.AI_DISABLE_SHORT !== undefined) document.getElementById('env-ai-disable-short').value = String(data.AI_DISABLE_SHORT).toLowerCase();
        if (data.USE_CLOSED_LOOP_MODELS !== undefined) document.getElementById('env-closed-loop').value = String(data.USE_CLOSED_LOOP_MODELS).toLowerCase();

        if (data.FUTURES_WS_ENABLED !== undefined) document.getElementById('env-fut-ws-enabled').value = String(data.FUTURES_WS_ENABLED).toLowerCase();
        if (data.SPOT_WS_ENABLED !== undefined) document.getElementById('env-spot-ws-enabled').value = String(data.SPOT_WS_ENABLED).toLowerCase();
        if (data.FUTURES_WS_TIMEFRAMES !== undefined) document.getElementById('env-fut-ws-tf').value = data.FUTURES_WS_TIMEFRAMES;
        if (data.SPOT_WS_TIMEFRAME !== undefined) document.getElementById('env-spot-ws-tf').value = data.SPOT_WS_TIMEFRAME;
        if (data.FUTURES_SYMBOL_LIMIT !== undefined) document.getElementById('env-fut-sym-limit').value = data.FUTURES_SYMBOL_LIMIT;
        if (data.SPOT_SYMBOL_LIMIT !== undefined) document.getElementById('env-spot-sym-limit').value = data.SPOT_SYMBOL_LIMIT;
        if (data.WS_CALLBACK_CONCURRENCY !== undefined) document.getElementById('env-ws-callback-conc').value = data.WS_CALLBACK_CONCURRENCY;
        if (data.RELOAD_MODELS_COOLDOWN_SEC !== undefined) document.getElementById('env-reload-cooldown').value = data.RELOAD_MODELS_COOLDOWN_SEC;

        if (data.SNAPSHOT_ENABLED !== undefined) document.getElementById('env-snapshot-enabled').value = String(data.SNAPSHOT_ENABLED).toLowerCase();
        if (data.EQUITY_SNAPSHOT_EVERY_SEC !== undefined) document.getElementById('env-equity-snap-sec').value = data.EQUITY_SNAPSHOT_EVERY_SEC;
        if (data.SNAPSHOT_RETENTION_DAYS !== undefined) document.getElementById('env-snap-ret-days').value = data.SNAPSHOT_RETENTION_DAYS;
        if (data.PAPER_TOURNAMENT_ENABLED !== undefined) document.getElementById('env-paper-tournament-enabled').value = String(data.PAPER_TOURNAMENT_ENABLED).toLowerCase();
        if (data.DISABLE_AUDIT_LAB !== undefined) document.getElementById('env-disable-audit-lab').value = String(data.DISABLE_AUDIT_LAB).toLowerCase();

        if (data.OI_LOG_ERRORS !== undefined) document.getElementById('env-oi-log-errors').value = String(data.OI_LOG_ERRORS).toLowerCase();
        if (data.OI_ERROR_THROTTLE_SEC !== undefined) document.getElementById('env-oi-throttle').value = data.OI_ERROR_THROTTLE_SEC;
        if (data.OI_SYMBOL_DELAY_SEC !== undefined) document.getElementById('env-oi-sym-delay').value = data.OI_SYMBOL_DELAY_SEC;
        if (data.OI_CYCLE_DELAY_SEC !== undefined) document.getElementById('env-oi-cycle-delay').value = data.OI_CYCLE_DELAY_SEC;
        if (data.OI_BASELINE_WINDOW_SEC !== undefined) document.getElementById('env-oi-baseline').value = data.OI_BASELINE_WINDOW_SEC;

} catch (e) { console.error("Failed to load DB config:", e); }
}

async function saveEnvSecrets() {
    if (!confirm("⚠️ CAUTION: Saving these settings will update your DB config (Zero-ENV).\n\nIf main.py is running, it will auto-reload config within seconds.\n\nProceed?")) return;
    const payload = {
        BINANCE_API_KEY: document.getElementById('env-binance-key').value,
        BINANCE_API_SECRET: document.getElementById('env-binance-secret').value,
        TELEGRAM_BOT_TOKEN: document.getElementById('env-tg-token').value,
        TELEGRAM_CHAT_ID: document.getElementById('env-tg-id').value,
        TELEGRAM_ERROR_CHAT_ID: document.getElementById('env-tg-err-id').value,
        USE_TESTNET: document.getElementById('env-use-testnet').value,
        ENABLE_LIVE_TRADING: document.getElementById('env-live-trading').value,
        RUN_MODE: document.getElementById('env-run-mode').value,
        HEARTBEAT_INTERVAL: document.getElementById('env-heartbeat').value,
        WS_CHUNK_SIZE: document.getElementById('env-chunk').value,
        TRAIL_TRIGGER: document.getElementById('env-trail').value,
        DEFAULT_RISK_PCT: document.getElementById('env-risk-pct').value,
        MAX_LEVERAGE: document.getElementById('env-max-lev').value,
        LEVERAGE_SCALP: document.getElementById('env-lev-scalp').value,
        LEVERAGE_SWING: document.getElementById('env-lev-swing').value,
        SL_MULTIPLIER: document.getElementById('env-sl-mult').value,
        TP_MULTIPLIER: document.getElementById('env-tp-mult').value,
        ENTRY_THRESHOLD: document.getElementById('env-entry-thresh').value,
        WATCHLIST_VOL_LIMIT: document.getElementById('env-vol-limit').value,
        WATCHLIST_GAINERS_LIMIT: document.getElementById('env-gain-limit').value,
        MAX_CONCURRENT_TASKS: document.getElementById('env-max-tasks').value,
        STARTUP_SCAN_LIMIT: document.getElementById('env-startup-limit').value
,
        BINANCE_FUTURES_REST_BASE: document.getElementById('env-fut-rest')?.value,
        BINANCE_FUTURES_WS_BASE: document.getElementById('env-fut-ws')?.value,

        BOT_NAME: document.getElementById('env-bot-name')?.value,
        DASHBOARD_URL: document.getElementById('env-dashboard-url')?.value,
        DASHBOARD_PUBLISH_URL: document.getElementById('env-dashboard-publish-url')?.value,
        DASHBOARD_PUBLISH_TOKEN: document.getElementById('env-dashboard-publish-token')?.value,
        DASHBOARD_ENVELOPE: document.getElementById('env-dashboard-envelope')?.value,

        AI_DISABLE_SHORT: document.getElementById('env-ai-disable-short')?.value,
        USE_CLOSED_LOOP_MODELS: document.getElementById('env-closed-loop')?.value,

        FUTURES_WS_ENABLED: document.getElementById('env-fut-ws-enabled')?.value,
        SPOT_WS_ENABLED: document.getElementById('env-spot-ws-enabled')?.value,
        FUTURES_WS_TIMEFRAMES: document.getElementById('env-fut-ws-tf')?.value,
        SPOT_WS_TIMEFRAME: document.getElementById('env-spot-ws-tf')?.value,
        FUTURES_SYMBOL_LIMIT: document.getElementById('env-fut-sym-limit')?.value,
        SPOT_SYMBOL_LIMIT: document.getElementById('env-spot-sym-limit')?.value,
        WS_CALLBACK_CONCURRENCY: document.getElementById('env-ws-callback-conc')?.value,
        RELOAD_MODELS_COOLDOWN_SEC: document.getElementById('env-reload-cooldown')?.value,

        SNAPSHOT_ENABLED: document.getElementById('env-snapshot-enabled')?.value,
        EQUITY_SNAPSHOT_EVERY_SEC: document.getElementById('env-equity-snap-sec')?.value,
        SNAPSHOT_RETENTION_DAYS: document.getElementById('env-snap-ret-days')?.value,
        PAPER_TOURNAMENT_ENABLED: document.getElementById('env-paper-tournament-enabled')?.value,
        DISABLE_AUDIT_LAB: document.getElementById('env-disable-audit-lab')?.value,

        OI_LOG_ERRORS: document.getElementById('env-oi-log-errors')?.value,
        OI_ERROR_THROTTLE_SEC: document.getElementById('env-oi-throttle')?.value,
        OI_SYMBOL_DELAY_SEC: document.getElementById('env-oi-sym-delay')?.value,
        OI_CYCLE_DELAY_SEC: document.getElementById('env-oi-cycle-delay')?.value,
        OI_BASELINE_WINDOW_SEC: document.getElementById('env-oi-baseline')?.value
    };
    await fetch('/api/env_secrets', { 
        method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify(payload) 
    });
    alert("✅ Settings Saved! (Bot will reload config if it's running.)");
}

// Webhooks
async function loadWebhookSettings() {
    const res = await fetch('/api/settings');
    const data = await res.json();
    document.getElementById('webhook-url').value = data.webhook_url || "";
    document.getElementById('webhook-sig').value = data.webhook_enable_signal || "FALSE";
    document.getElementById('webhook-trade').value = data.webhook_enable_trade || "FALSE";
}

async function saveWebhookSettings() {
    const payload = {
        webhook_url: document.getElementById('webhook-url').value,
        webhook_enable_signal: document.getElementById('webhook-sig').value,
        webhook_enable_trade: document.getElementById('webhook-trade').value
    };
    await fetch('/api/settings', { 
        method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify(payload) 
    });
    alert("✅ API Bridge Settings Saved!");
}

async function testWebhook() {
    const url = document.getElementById('webhook-url').value;
    if(!url) return alert("Please enter a URL first.");
    try {
        const res = await fetch('/api/test_webhook', {
            method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify({ url: url })
        });
        const data = await res.json();
        alert(data.msg);
    } catch(e) {
        alert("⚠️ Failed to connect to Bot Server.");
        console.error(e);
    }
}

// ===========================================
// 7. INSPECTOR & SYSTEM HEALTH
// ===========================================
async function inspectCoin() {
    const symbol = document.getElementById('insp-symbol').value.toUpperCase();
    if(!symbol) return alert("Enter a symbol first!");

    const btn = document.querySelector('button[onclick="inspectCoin()"]');
    const originalText = btn.innerText;
    btn.innerText = "Scanning...";
    btn.disabled = true;

    try {
        const res = await fetch('/api/inspect', {
            method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify({ symbol: symbol })
        });
        const data = await res.json();

        if (data.status === 'error') {
            alert(data.msg);
        } else {
            document.getElementById('insp-result').style.display = 'block';
            document.getElementById('insp-price').innerText = `$${data.price}`;
            const decEl = document.getElementById('insp-decision');
            decEl.innerText = data.decision.gate;
            document.getElementById('insp-reason').innerText = data.decision.reason;

            if (data.decision.gate === 'PASS') decEl.style.color = '#2ea043';
            else if (data.decision.gate === 'BLOCK') decEl.style.color = '#da3633';
            else decEl.style.color = '#d29922';

            document.getElementById('set-rsi').innerText = data.settings.max_rsi;
            const rsiEl = document.getElementById('val-rsi');
            rsiEl.innerText = data.indicators.rsi;
            rsiEl.style.color = data.indicators.rsi > data.settings.max_rsi ? '#da3633' : '#e6edf3';

            document.getElementById('set-adx').innerText = data.settings.min_adx;
            const adxEl = document.getElementById('val-adx');
            adxEl.innerText = data.indicators.adx;
            adxEl.style.color = data.indicators.adx < data.settings.min_adx ? '#da3633' : '#e6edf3';

            const atrEl = document.getElementById('val-atr');
            atrEl.innerText = (data.indicators.atr_pct * 100).toFixed(2) + '%';
            atrEl.style.color = data.indicators.atr_pct < data.settings.min_atr ? '#da3633' : '#e6edf3';

            const hasVol = data.indicators.volume_spike;
            const volEl = document.getElementById('val-vol');
            volEl.innerText = hasVol ? "YES 🚀" : "NO";
            volEl.style.color = hasVol ? '#2ea043' : '#8b949e';
        }
    } catch (e) {
        console.error(e);
        alert("Scan Failed. Check console.");
    } finally {
        btn.innerText = originalText;
        btn.disabled = false;
    }
}

async function fetchSystemHealth() {
    try {
        const res = await fetch('/api/system_health');
        const data = await res.json();

        // Online Status
        const dot = document.getElementById('sys-status-dot');
        const txt = document.getElementById('sys-status-text');
        const seen = document.getElementById('sys-last-seen');

        if (data.online) {
            dot.className = "dot green";
            txt.innerText = "RUNNING";
            txt.style.color = "#2ea043";
        } else {
            dot.className = "dot red";
            txt.innerText = "STALLED / OFFLINE";
            txt.style.color = "#da3633";
        }
        seen.innerText = `Last heartbeat: ${data.last_seen_seconds}s ago`;

        // Latency
        document.getElementById('sys-latency').innerText = data.latency + " ms";
        let latColor = '#2ea043';
        if(data.latency > 500) latColor = '#d29922';
        if(data.latency > 1000) latColor = '#da3633';
        document.getElementById('bar-latency').style.width = Math.min(data.latency / 10, 100) + "%";
        document.getElementById('bar-latency').style.backgroundColor = latColor;

        // Resources
        document.getElementById('sys-cpu-val').innerText = data.cpu + "%";
        document.getElementById('bar-cpu').style.width = data.cpu + "%";
        
        document.getElementById('sys-ram-val').innerText = data.ram + "%";
        document.getElementById('bar-ram').style.width = data.ram + "%";

        document.getElementById('sys-db-size').innerText = data.db_size;


// Kill Switch + ExecMon
try {
    const ksEl = document.getElementById('sys-kill-switch');
    const krEl = document.getElementById('sys-kill-reason');
    const emEl = document.getElementById('sys-execmon-age');
    if (ksEl && data.kill_switch !== undefined) ksEl.innerText = String(data.kill_switch);
    if (krEl && data.kill_switch_reason !== undefined) krEl.innerText = (data.kill_switch_reason || '-') || '-';
    if (emEl && data.execmon_last_seen_seconds !== undefined) emEl.innerText = String(data.execmon_last_seen_seconds);
} catch(e) { /* ignore */ }



        // Mirror key status to Env Settings tab (so user can control everything from there)
        try {
            const s1 = document.getElementById('env-sys-status');
            const s2 = document.getElementById('env-sys-last');
            const ks = document.getElementById('env-sys-kill');
            const kr = document.getElementById('env-sys-kill-reason');
            const em = document.getElementById('env-sys-exec-age');
            const lt = document.getElementById('env-sys-lat');

            if (s1) s1.innerText = data.online ? "RUNNING" : "OFFLINE";
            if (s2) s2.innerText = data.last_seen_seconds !== undefined ? ("(last seen " + data.last_seen_seconds + "s)") : "";
            if (ks && data.kill_switch !== undefined) ks.innerText = String(data.kill_switch);
            if (kr && data.kill_switch_reason !== undefined) kr.innerText = (data.kill_switch_reason || '-') || '-';
            if (em && data.execmon_last_seen_seconds !== undefined) em.innerText = String(data.execmon_last_seen_seconds);
            if (lt && data.latency !== undefined) lt.innerText = String(data.latency);
        } catch(e) { /* ignore */ }

    } catch(e) { console.error(e); }
}

// ===========================================
// 8. COMMANDS
// ===========================================
async function closePosition(sym) {
    if(confirm('Close ' + sym + '?')) {
        await fetch('/api/command', { 
            method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify({cmd:'CLOSE_TRADE', params:{symbol:sym}}) 
        });
        setTimeout(fetchPositions, 1000);
    }
}

async function executeManualTrade() {
    const symbol = document.getElementById('m-symbol').value.toUpperCase();
    const amount = document.getElementById('m-amount').value;
    const market = document.getElementById('m-market').value;
    const side = document.getElementById('m-side').value;
    if(!symbol || !amount) return alert("Please enter Symbol and Amount");
    if(!confirm(`⚠️ CONFIRM MANUAL TRADE:\n\n${side} ${symbol}\nAmount: $${amount}\nMarket: ${market.toUpperCase()}\n\nProceed?`)) return;
    await fetch('/api/command', {
        method: 'POST', headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({ cmd: 'MANUAL_TRADE', params: { symbol: symbol, amount: amount, market_type: market, side: side } })
    });
    alert("✅ Trade Command Sent!");
}

async function executeManualTrain() {
    const symbol = document.getElementById('t-symbol').value.toUpperCase();
    const timeVal = document.getElementById('t-time').value;
    if(!symbol || !timeVal) return alert("Please enter Symbol and Time");
    await fetch('/api/command', {
        method: 'POST', headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({ cmd: 'LEARN_PUMP', params: { symbol: symbol, timestamp: timeVal } })
    });
    alert("✅ Learning Task Queued!");
}

async function requestAnalysis() {
    const symbol = document.getElementById('m-symbol').value.toUpperCase();
    const market = document.getElementById('m-market').value;
    if(!symbol) return alert("Please enter a Symbol");
    await fetch('/api/command', {
        method: 'POST', headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({ cmd: 'CHECK_SIGNAL', params: { symbol: symbol, market_type: market } })
    });
    alert("🔎 Analysis Requested!");
}

async function forceRetrain() {
    if(confirm("Stop bot and retrain models now?")) {
        await fetch('/api/command', {
             method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify({cmd:'RELOAD_MODELS'}) 
        });
        alert("Command Sent!");
    }
}

// ===========================================
// 9. NAVIGATION
// ===========================================
function switchTab(tabName) {
    document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
    document.querySelectorAll('.tab-content').forEach(c => c.classList.remove('active'));
    
    const btn = document.querySelector(`button[onclick="switchTab('${tabName}')"]`);
    if(btn) btn.classList.add('active');
    
    const content = document.getElementById(`view-${tabName}`);
    if(content) content.classList.add('active');

    if(tabName === 'analytics') { loadEquityChart(); fetchAdvancedStats(); }
    if(tabName === 'config') { loadCoreTradingSettings(); loadAdvancedConfig(); }
    if(tabName === 'lab') loadLearningMatrix();
    if(tabName === 'secrets') loadEnvSecrets();
    if(tabName === 'report') loadFullReport();
    if(tabName === 'api') loadWebhookSettings();
    if(tabName === 'system') fetchSystemHealth();
    if(tabName === 'allsettings') loadAllSettings();
    if(tabName === 'paper') loadPaperArena();
}

// ===========================================
// 9B. SYSTEM CONTROLS (Kill Switch / Restart)
// ===========================================
async function clearKillSwitch() {
    if(!confirm("Clear kill_switch? Bot will be allowed to trade again.")) return;
    try {
        const res = await fetch('/api/control/clear_kill_switch', { method:'POST' });
        const data = await res.json();
        if(!data.ok) alert("Failed to clear kill_switch");
        else alert("✅ kill_switch cleared");
        fetchSystemHealth();
    } catch(e) {
        console.error(e);
        alert("Failed. Check console.");
    }
}

async function restartBot() {
    if(!confirm("Restart BOT process now?\n\nNOTE: This will request the process to exit. If you're not running a loop/supervisor, it will NOT auto-start again.")) return;
    try {
        const res = await fetch('/api/control/restart_bot', { method:'POST' });
        const data = await res.json();
        alert("✅ Restart requested. Token: " + (data.req || ""));
    } catch(e) {
        console.error(e);
        alert("Failed. Check console.");
    }
}

async function restartMonitor() {
    if(!confirm("Restart Execution Monitor now?\n\nNOTE: This will request the monitor to exit. If you're not running a loop/supervisor, it will NOT auto-start again.")) return;
    try {
        const res = await fetch('/api/control/restart_monitor', { method:'POST' });
        const data = await res.json();
        alert("✅ Restart requested. Token: " + (data.req || ""));
    } catch(e) {
        console.error(e);
        alert("Failed. Check console.");
    }
}


// ===========================================
// 9C. ALL SETTINGS (DB)
// ===========================================
function _escapeHtml(s) {
    return String(s)
        .replaceAll('&','&amp;')
        .replaceAll('<','&lt;')
        .replaceAll('>','&gt;')
        .replaceAll('"','&quot;')
        .replaceAll("'",'&#039;');
}

async function loadAllSettings() {
    try {
        const res = await fetch('/api/settings_all');
        const data = await res.json();
        const keys = Object.keys(data || {}).sort((a,b)=>a.localeCompare(b));
        const body = document.getElementById('allset-body');
        if(!body) return;
        body.innerHTML = '';

        keys.forEach((k, i) => {
            const v = (data[k] === null || data[k] === undefined) ? '' : String(data[k]);
            const id = `allset-${i}`;
            const tr = document.createElement('tr');
            tr.setAttribute('data-key', k);
            tr.innerHTML = `
                <td style="padding:10px; border-bottom:1px solid #21262d;"><code>${_escapeHtml(k)}</code></td>
                <td style="padding:10px; border-bottom:1px solid #21262d;">
                    <input id="${id}" class="dark-input" value="${_escapeHtml(v)}" data-key="${_escapeHtml(k)}" data-orig="${_escapeHtml(v)}" style="width:100%;">
                </td>
                <td style="padding:10px; border-bottom:1px solid #21262d; text-align:right;">
                    <button class="btn btn-secondary" onclick="saveOneSetting('${id}')" style="width:auto;">Save</button>
                </td>
            `;
            body.appendChild(tr);
        });

        // Search filter
        const s = document.getElementById('allset-search');
        if(s) {
            s.oninput = () => {
                const q = String(s.value || '').toLowerCase().trim();
                document.querySelectorAll('#allset-body tr').forEach(tr => {
                    const key = String(tr.getAttribute('data-key') || '').toLowerCase();
                    tr.style.display = (!q || key.includes(q)) ? '' : 'none';
                });
            };
        }
    } catch(e) {
        console.error(e);
        alert("Failed to load All Settings");
    }
}

async function saveOneSetting(inputId) {
    const el = document.getElementById(inputId);
    if(!el) return;
    const key = el.getAttribute('data-key');
    const val = el.value;
    const payload = {};
    payload[key] = val;

    try {
        const res = await fetch('/api/settings', { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify(payload) });
        const data = await res.json();
        if(data.status !== 'ok') {
            alert("Save failed: " + (data.detail || data.error || "Unknown error"));
            return;
        }
        el.setAttribute('data-orig', val);
        alert("✅ Saved: " + key);
    } catch(e) {
        console.error(e);
        alert("Save failed. Check console / server logs for details.");
    }
}

async function saveAllSettings() {
    const inputs = document.querySelectorAll('#allset-body input[data-key]');
    const payload = {};
    let changed = 0;
    inputs.forEach(el => {
        const key = el.getAttribute('data-key');
        const val = el.value;
        const orig = el.getAttribute('data-orig');
        if(String(val) !== String(orig)) {
            payload[key] = val;
            changed += 1;
        }
    });
    if(changed === 0) return alert("No changes to save.");

    try {
        const res = await fetch('/api/settings', { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify(payload) });
        const data = await res.json();
        if(data.status !== 'ok') {
            alert("Save failed: " + (data.detail || data.error || "Unknown error"));
            return;
        }
        // update orig
        inputs.forEach(el => {
            const key = el.getAttribute('data-key');
            if(payload[key] !== undefined) el.setAttribute('data-orig', el.value);
        });
        alert("✅ Saved " + changed + " setting(s)");
    } catch(e) {
        console.error(e);
        alert("Save failed. Check console / server logs for details.");
    }
}

// ===========================================
// 10. INIT
// ===========================================
document.addEventListener('DOMContentLoaded', () => {
    loadSettings();
    fetchPositions();
    fetchLogs();
    fetchStats();
    loadSignalInbox({ status: '', limit: 30 });
    fetchSystemHealth();

    setInterval(fetchPositions, 5000);
    setInterval(fetchLogs, 2000);
    setInterval(fetchStats, 5000);
    setInterval(fetchSystemHealth, 10000);

    // Fallback refresh for signals (in case WS drops)
    setInterval(() => { if (ws.readyState !== WebSocket.OPEN) loadSignalInbox({ status: '', limit: 30 }); }, 15000);
});
// ===========================================
// 11. HISTORICAL SNAPSHOT (Manual Tab) - UTC
// ===========================================
function _pad2(n) { return String(n).padStart(2, '0'); }

function localToUtcMinute(localValue) {
    // localValue: "YYYY-MM-DDTHH:MM"
    const d = new Date(localValue);
    if (isNaN(d.getTime())) return null;
    const y = d.getUTCFullYear();
    const m = _pad2(d.getUTCMonth() + 1);
    const day = _pad2(d.getUTCDate());
    const hh = _pad2(d.getUTCHours());
    const mm = _pad2(d.getUTCMinutes());
    return `${y}-${m}-${day} ${hh}:${mm}`;
}

function _snapshotShow(html) {
    const box = document.getElementById('snapshot-result');
    if (!box) return;
    box.style.display = 'block';
    box.innerHTML = html;
}

async function runSnapshotAt() {
    const symEl = document.getElementById('snap-symbol');
    const atEl = document.getElementById('snap-at');
    const marketEl = document.getElementById('snap-market');
    const intEl = document.getElementById('snap-interval');

    if (!symEl || !atEl || !marketEl || !intEl) return;

    const symbol = (symEl.value || '').trim().toUpperCase();
    const atLocal = atEl.value;
    const at_utc = localToUtcMinute(atLocal);
    const market_type = marketEl.value;
    const interval = intEl.value;

    if (!symbol) return alert("Enter symbol (e.g. SOLUSDT)");
    if (!at_utc) return alert("Pick a valid date/time");

    const res = await fetch('/api/snapshot', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({ mode: 'at', symbol, market_type, interval, at_utc })
    });

    const data = await res.json();
    if (data.status !== 'ok') {
        _snapshotShow(`<div style="color:#da3633;">❌ ${data.msg || 'Snapshot failed'}</div>`);
        return;
    }

    renderSnapshot(data, { at_utc });
}

async function runSnapshotRange() {
    const symEl = document.getElementById('snap-symbol');
    const fromEl = document.getElementById('snap-from');
    const toEl = document.getElementById('snap-to');
    const marketEl = document.getElementById('snap-market');
    const intEl = document.getElementById('snap-interval');

    if (!symEl || !fromEl || !toEl || !marketEl || !intEl) return;

    const symbol = (symEl.value || '').trim().toUpperCase();
    const from_utc = localToUtcMinute(fromEl.value);
    const to_utc = localToUtcMinute(toEl.value);
    const market_type = marketEl.value;
    const interval = intEl.value;

    if (!symbol) return alert("Enter symbol (e.g. SOLUSDT)");
    if (!from_utc || !to_utc) return alert("Pick FROM and TO date/time");

    const res = await fetch('/api/snapshot', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({ mode: 'range', symbol, market_type, interval, from_utc, to_utc })
    });

    const data = await res.json();
    if (data.status !== 'ok') {
        _snapshotShow(`<div style="color:#da3633;">❌ ${data.msg || 'Snapshot failed'}</div>`);
        return;
    }

    renderSnapshot(data, { from_utc, to_utc, isRange: true });
}

function renderSnapshot(data, meta) {
    const ind = data.indicators || {};
    const dec = data.decision || {};
    const set = data.settings || {};

    const gate = dec.gate || 'N/A';
    const gateColor = gate === 'PASS' ? '#2ea043' : (gate === 'WAIT' ? '#d29922' : '#da3633');
    const reasons = (dec.reasons || []).map(r => `<li>${r}</li>`).join('') || '<li>—</li>';

    let seriesHtml = '';
    if (meta && meta.isRange && Array.isArray(data.series)) {
        const tail = data.series.slice(-25);
        const rows = tail.map(r => `
            <tr style="border-bottom:1px solid #21262d;">
                <td style="padding:6px;">${(r.ts || '').split('T')[1]?.slice(0,5) || ''}</td>
                <td style="padding:6px;">${Number(r.close).toFixed(4)}</td>
                <td style="padding:6px;">${Number(r.rsi).toFixed(1)}</td>
                <td style="padding:6px;">${Number(r.adx).toFixed(1)}</td>
                <td style="padding:6px;">${(Number(r.atr_pct)*100).toFixed(3)}%</td>
                <td style="padding:6px;">${r.vol_spike ? 'YES' : 'NO'}</td>
            </tr>
        `).join('');

        seriesHtml = `
            <div style="margin-top:12px; color:#8b949e; font-size:0.85rem;">Last 25 candles (UTC time):</div>
            <div style="overflow:auto; max-height:240px; border:1px solid #30363d; border-radius:8px; margin-top:8px;">
                <table style="width:100%; border-collapse:collapse; font-size:0.85rem; color:#e6edf3;">
                    <thead style="position:sticky; top:0; background:#0d1117; border-bottom:1px solid #30363d;">
                        <tr>
                            <th style="text-align:left; padding:6px; color:#8b949e;">Time</th>
                            <th style="text-align:left; padding:6px; color:#8b949e;">Close</th>
                            <th style="text-align:left; padding:6px; color:#8b949e;">RSI</th>
                            <th style="text-align:left; padding:6px; color:#8b949e;">ADX</th>
                            <th style="text-align:left; padding:6px; color:#8b949e;">ATR%</th>
                            <th style="text-align:left; padding:6px; color:#8b949e;">VolSpike</th>
                        </tr>
                    </thead>
                    <tbody>${rows}</tbody>
                </table>
            </div>
        `;
    }

    _snapshotShow(`
        <div style="display:flex; justify-content:space-between; gap:10px; align-items:center;">
            <div style="font-weight:700; font-size:1.05rem;">${data.symbol} • ${data.market_type.toUpperCase()} • ${data.interval}</div>
            <div style="padding:6px 10px; border-radius:999px; border:1px solid #30363d; color:${gateColor}; font-weight:700;">
                ${gate}
            </div>
        </div>

        <div style="margin-top:6px; color:#8b949e; font-size:0.85rem;">
            Requested: <b>${meta.isRange ? (meta.from_utc + ' → ' + meta.to_utc) : meta.at_utc}</b> (UTC)
            • Last candle: <b>${(data.last_candle_utc || '').replace('T',' ').slice(0,16)}</b> (UTC)
        </div>

        <div style="margin-top:12px; display:grid; grid-template-columns:repeat(4, 1fr); gap:10px;">
            <div style="background:#0d1117; border:1px solid #30363d; border-radius:10px; padding:10px;">
                <div style="color:#8b949e; font-size:0.8rem;">Price</div>
                <div style="font-weight:700;">${Number(data.price).toFixed(6)}</div>
            </div>
            <div style="background:#0d1117; border:1px solid #30363d; border-radius:10px; padding:10px;">
                <div style="color:#8b949e; font-size:0.8rem;">RSI (max ${set.max_rsi})</div>
                <div style="font-weight:700;">${Number(ind.rsi).toFixed(1)}</div>
            </div>
            <div style="background:#0d1117; border:1px solid #30363d; border-radius:10px; padding:10px;">
                <div style="color:#8b949e; font-size:0.8rem;">ADX (min ${set.min_adx})</div>
                <div style="font-weight:700;">${Number(ind.adx).toFixed(1)}</div>
            </div>
            <div style="background:#0d1117; border:1px solid #30363d; border-radius:10px; padding:10px;">
                <div style="color:#8b949e; font-size:0.8rem;">ATR% (min ${(Number(set.min_atr)*100).toFixed(2)}%)</div>
                <div style="font-weight:700;">${(Number(ind.atr_pct)*100).toFixed(3)}%</div>
            </div>
        </div>

        <div style="margin-top:10px; display:flex; gap:10px; align-items:center; color:#8b949e;">
            <div style="padding:6px 10px; border:1px solid #30363d; border-radius:999px;">
                Vol Spike: <b style="color:#e6edf3;">${ind.volume_spike ? 'YES' : 'NO'}</b>
            </div>
            <div style="padding:6px 10px; border:1px solid #30363d; border-radius:999px;">
                Pump Score: <b style="color:#e6edf3;">${ind.pump_score ?? 0}</b>/100
            </div>
        </div>

        <div style="margin-top:10px; color:#8b949e; font-size:0.85rem;">Gate reasons:</div>
        <ul style="margin:6px 0 0 18px; color:#e6edf3; font-size:0.9rem;">${reasons}</ul>

        ${seriesHtml}
    `);
}

// ===========================================
// PAPER ARENA (Leaderboard + Recommendations)
// ===========================================
async function loadPaperArena() {
    try {
        const days = parseInt(document.getElementById('paper-days')?.value || '7');
        const horizon = document.getElementById('paper-horizon')?.value || '1h';
        const market = document.getElementById('paper-market')?.value || 'futures';

        const res = await fetch(`/api/paper/leaderboard?days=${days}&horizon=${encodeURIComponent(horizon)}&market=${encodeURIComponent(market)}`);
        const j = await res.json();
        const box = document.getElementById('paper-leaderboard');
        if (!box) return;

        if (!j.items || j.items.length === 0) {
            box.innerHTML = '<div class="muted">No evaluated paper results yet. Enable paper_tournament_enabled + paper_eval_enabled and wait.</div>';
        } else {
            let html = '<table class="dark-table"><thead><tr><th>Strategy</th><th>Trades</th><th>Win%</th><th>Avg Return%</th></tr></thead><tbody>';
            j.items.forEach(r => {
                html += `<tr><td>${escapeHtml(r.strategy_tag)}</td><td>${r.trades}</td><td>${r.win_rate}</td><td>${r.avg_return}</td></tr>`;
            });
            html += '</tbody></table>';
            box.innerHTML = html;
        }

        await loadPaperRecs();

    } catch (e) {
        console.error(e);
    }
}

async function generatePaperRecs() {
    const days = parseInt(document.getElementById('paper-days')?.value || '7');
    const horizon = document.getElementById('paper-horizon')?.value || '1h';
    const market = document.getElementById('paper-market')?.value || 'futures';
    const res = await fetch('/api/paper/recommendations/generate', {
        method: 'POST',
        headers: {'Content-Type':'application/json'},
        body: JSON.stringify({days, horizon, market})
    });
    await res.json();
    await loadPaperRecs();
}

async function loadPaperRecs() {
    const res = await fetch('/api/paper/recommendations');
    const j = await res.json();
    const box = document.getElementById('paper-recs');
    if (!box) return;

    const items = j.items || [];
    if (items.length === 0) {
        box.innerHTML = '<div class="muted">No recommendations yet. Click Generate.</div>';
        return;
    }

    let html = '';
    items.forEach(r => {
        html += `
        <div class="rec-card">
          <div class="rec-top">
            <div><b>${escapeHtml(r.key || '')}</b> <span class="muted">(${escapeHtml(r.category || '')})</span></div>
            <div class="muted">${escapeHtml(r.status || '')}</div>
          </div>
          <div class="muted" style="margin-top:6px;">${escapeHtml(r.reason || '')}</div>
          <div style="margin-top:8px;">
            <span class="pill">current: ${escapeHtml(r.current_value || '')}</span>
            <span class="pill">suggested: ${escapeHtml(r.suggested_value || '')}</span>
          </div>
          <div style="margin-top:10px; display:flex; gap:8px;">
            <button class="btn btn-primary" onclick="approvePaperRec(${r.id})">Approve</button>
            <button class="btn btn-secondary" onclick="rejectPaperRec(${r.id})">Reject</button>
          </div>
        </div>`;
    });
    box.innerHTML = html;
}

async function approvePaperRec(id) {
    await fetch('/api/paper/recommendations/approve', {
        method:'POST',
        headers:{'Content-Type':'application/json'},
        body: JSON.stringify({id})
    });
    await loadPaperArena();
}
async function rejectPaperRec(id) {
    await fetch('/api/paper/recommendations/reject', {
        method:'POST',
        headers:{'Content-Type':'application/json'},
        body: JSON.stringify({id})
    });
    await loadPaperArena();
}

function escapeHtml(str) {
    if (str === null || str === undefined) return '';
    return String(str).replaceAll('&','&amp;').replaceAll('<','&lt;').replaceAll('>','&gt;');
}
function toUtcYmdHm(datetimeLocalValue) {
  // datetime-local returns local time; convert to UTC string YYYY-MM-DD HH:MM
  if (!datetimeLocalValue) return null;
  const d = new Date(datetimeLocalValue); // interpreted as local
  const yyyy = d.getUTCFullYear();
  const mm = String(d.getUTCMonth() + 1).padStart(2, "0");
  const dd = String(d.getUTCDate()).padStart(2, "0");
  const hh = String(d.getUTCHours()).padStart(2, "0");
  const mi = String(d.getUTCMinutes()).padStart(2, "0");
  return `${yyyy}-${mm}-${dd} ${hh}:${mi}`;
}
