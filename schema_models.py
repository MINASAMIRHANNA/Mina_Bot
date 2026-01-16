"""
schema_models.py

Strict (but backward-compatible) schema validation for dashboard↔bot payloads.
Designed for single-server deployments: fast, minimal dependencies, no breaking changes.

- Accepts extra keys (so Pump Hunter / future fields won't break).
- Normalizes side/decision to BUY/SELL consistently.
- Works with both Pydantic v1 and v2.
"""
from __future__ import annotations
from typing import Any, Dict, Optional

import re
# --- Pydantic v1/v2 compatibility ---
try:
    from pydantic import BaseModel, Field, ConfigDict, field_validator
    _PYDANTIC_V2 = True
except Exception:  # pragma: no cover
    from pydantic import BaseModel, Field, validator as _validator  # type: ignore
    _PYDANTIC_V2 = False

    def field_validator(*fields, **kwargs):  # type: ignore
        return _validator(*fields, **kwargs)


def normalize_symbol(v: str) -> str:
    return (v or "").strip().upper()


def normalize_side(raw: Any) -> Optional[str]:
    """
    Normalize various encodings to BUY/SELL.
    Accepts: BUY/SELL, LONG/SHORT, 1/-1, BULL/BEAR, STRONG_LONG/STRONG_SHORT, etc.
    Returns BUY/SELL or None.
    """
    if raw is None:
        return None
    s = str(raw).strip().upper()
    if not s:
        return None

    # direct matches
    if s in {"BUY", "LONG", "BULL", "1", "+1"}:
        return "BUY"
    if s in {"SELL", "SHORT", "BEAR", "-1"}:
        return "SELL"

    # token-based parsing (avoids bugs like 'STRONG_SHORT' containing 'LONG' in 'STRONG')
    tokens = [t for t in re.split(r"[^A-Z]+", s) if t]
    buy_tokens = {"BUY", "LONG", "BULL"}
    sell_tokens = {"SELL", "SHORT", "BEAR"}

    has_buy = any(t in buy_tokens for t in tokens)
    has_sell = any(t in sell_tokens for t in tokens)

    # Ambiguous => reject
    if has_buy and has_sell:
        return None
    if has_sell:
        return "SELL"
    if has_buy:
        return "BUY"
    return None


class ExecuteSignalPayload(BaseModel):
    """
    Payload for EXECUTE_SIGNAL coming from dashboard/DB commands.
    Backward compatible: allows many key variants and extra fields.
    """
    symbol: str = Field(..., description="Trading pair symbol, e.g. BTCUSDT")
    # optional variants:
    side: Optional[Any] = None
    decision: Optional[Any] = None
    action: Optional[Any] = None
    direction: Optional[Any] = None
    signal: Optional[Any] = None
    order_side: Optional[Any] = None
    position_side: Optional[Any] = None

    market_type: Optional[str] = None
    market: Optional[str] = None
    quantity: Optional[float] = None
    usd_amount: Optional[float] = None
    sl: Optional[float] = None
    stop_loss: Optional[float] = None
    tp: Optional[float] = None
    take_profit: Optional[float] = None
    leverage: Optional[int] = None

    # allow extra (pump hunter fields etc.)
    if _PYDANTIC_V2:
        model_config = ConfigDict(extra="allow")
    else:  # pydantic v1
        class Config:
            extra = "allow"

    @field_validator("symbol")
    @classmethod
    def _v_symbol(cls, v: str) -> str:
        v = normalize_symbol(v)
        if not v:
            raise ValueError("symbol is required")
        return v

    def normalized_market_type(self) -> str:
        mt = (self.market_type or self.market or "futures")
        return str(mt).strip().lower() or "futures"

    def normalized_side(self) -> str:
        # choose first present
        for k in ("side", "decision", "action", "direction", "signal", "order_side", "position_side", "ai_vote"):
            val = getattr(self, k, None)
            if val is not None:
                out = normalize_side(val)
                if out:
                    return out
        raise ValueError("Invalid side: missing/empty (need LONG/SHORT or BUY/SELL)")


class SignalInboxItem(BaseModel):
    """
    Strict-ish schema for items pushed to dashboard signal inbox.
    """
    symbol: str
    decision: str  # LONG/SHORT/WAIT
    confidence: float = 0.0
    ai_vote: Optional[str] = None
    ai_confidence: Optional[float] = None
    status: str = "PENDING_APPROVAL"
    market_type: str = "futures"
    payload: Dict[str, Any] = Field(default_factory=dict)

    if _PYDANTIC_V2:
        model_config = ConfigDict(extra="allow")
    else:
        class Config:
            extra = "allow"

    @field_validator("symbol")
    @classmethod
    def _v_symbol2(cls, v: str) -> str:
        v = normalize_symbol(v)
        if not v:
            raise ValueError("symbol is required")
        return v

    @field_validator("decision")
    @classmethod
    def _v_decision(cls, v: str) -> str:
        s = (v or "").strip().upper()
        if s not in {"LONG", "SHORT", "WAIT"}:
            raise ValueError(f"invalid decision: {v}")
        return s
