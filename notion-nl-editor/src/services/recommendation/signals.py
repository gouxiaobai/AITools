from typing import Any, Dict, List, Optional, Tuple

from services.recommendation.common import safe_mean, safe_stdev
from services.recommendation.data_prep import TradePoint
from services.recommendation.strategy_registry import StrategyEvalRequest, get_strategy_ids, get_strategy_registry, register_strategy
from stores.kline_store import KBar


def _clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def _confidence_from_history(sample_count: int, returns: List[float], realized_values: List[float]) -> float:
    sample_score = min(sample_count / 40.0, 1.0) * 0.35
    vol = safe_stdev(returns)
    vol_score = (1.0 - min(vol / 0.06, 1.0)) * 0.25
    realized_avg = safe_mean(realized_values)
    realized_score = 0.15 if realized_avg > 0 else 0.05
    trend_consistency = (1.0 - min(abs(safe_mean(returns)) / 0.12, 1.0)) * 0.15
    data_score = 0.10 if sample_count > 0 else 0.0
    return _clamp(sample_score + vol_score + realized_score + trend_consistency + data_score, 0.0, 1.0)


def _confidence_level(score: float) -> str:
    if score >= 0.75:
        return "HIGH"
    if score >= 0.55:
        return "MEDIUM"
    return "LOW"


def _level_rank(level: str) -> int:
    return {"LOW": 0, "MEDIUM": 1, "HIGH": 2}.get(level, 0)


def recommend_from_points(
    current_price: Optional[float],
    current_cost: Optional[float],
    points: List[TradePoint],
    allow_small_sample: bool,
    min_confidence: str,
    param_cfg: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    cfg = param_cfg or {}
    trend_threshold = float(cfg.get("trend_threshold", 0.015))
    vol_cap = float(cfg.get("vol_cap", 0.10))
    band_low = float(cfg.get("band_low", 0.01))
    band_high = float(cfg.get("band_high", 0.12))
    stop_mult = float(cfg.get("stop_mult", 1.8))
    rr_min = float(cfg.get("rr_min", 0.8))
    if current_price is None or current_price <= 0:
        return {
            "action": "HOLD",
            "buy_price": None,
            "sell_price": None,
            "stop_price": None,
            "position_delta": 0.0,
            "confidence": "LOW",
            "mode": "TREND_FALLBACK",
            "reason": "missing current price",
            "sample_count": len(points),
        }

    valid_points = [p for p in points if p.price is not None and p.price > 0]
    sample_count = len(valid_points)
    mode = "FULL_MODEL" if sample_count >= 20 else "TREND_FALLBACK"
    if sample_count < 20 and not allow_small_sample:
        return {
            "action": "HOLD",
            "buy_price": round(current_price, 4),
            "sell_price": round(current_price, 4),
            "stop_price": round(current_price * 0.98, 4),
            "position_delta": 0.0,
            "confidence": "LOW",
            "mode": "TREND_FALLBACK",
            "reason": "sample too small",
            "sample_count": sample_count,
        }

    prices = [p.price for p in valid_points if p.price is not None]
    returns: List[float] = []
    for i in range(1, len(prices)):
        prev = prices[i - 1]
        curr = prices[i]
        if prev > 0:
            returns.append((curr - prev) / prev)

    realized_values = [p.realized for p in valid_points if p.realized is not None]
    conf_score = _confidence_from_history(sample_count, returns[-30:], realized_values[-30:])
    conf_level = _confidence_level(conf_score)
    if mode == "TREND_FALLBACK" and conf_level == "HIGH":
        conf_level = "MEDIUM"

    vol = safe_stdev(returns[-20:]) if returns else 0.02
    if vol > vol_cap:
        return {
            "action": "HOLD",
            "buy_price": round(current_price * 0.99, 4),
            "sell_price": round(current_price * 1.01, 4),
            "stop_price": round(current_price * 0.97, 4),
            "position_delta": 0.0,
            "confidence": conf_level,
            "mode": mode,
            "reason": "volatility too high",
            "sample_count": sample_count,
        }

    ma_window = min(20, len(prices))
    moving_avg = safe_mean(prices[-ma_window:]) if ma_window > 0 else current_price
    trend = (current_price / moving_avg - 1.0) if moving_avg > 0 else 0.0
    band = _clamp(max(vol * 1.2, band_low), band_low, band_high)

    buy_price = current_price * (1.0 - band)
    sell_price = current_price * (1.0 + band)
    stop_price = current_price * (1.0 - band * stop_mult)
    if current_cost is not None and current_cost > 0:
        buy_price = min(buy_price, current_cost * 0.995)
        sell_price = max(sell_price, current_cost * 1.005)

    action = "HOLD"
    reason = "neutral"
    if trend > trend_threshold:
        action = "BUY"
        reason = "up trend"
    elif trend < -trend_threshold:
        action = "SELL"
        reason = "down trend"

    expected_up = max(0.0, (sell_price - current_price) / current_price)
    expected_down = max(0.0, (current_price - stop_price) / current_price)
    if expected_up <= expected_down * rr_min:
        action = "HOLD"
        reason = "risk/reward not favorable"
    if _level_rank(conf_level) < _level_rank(min_confidence):
        action = "HOLD"
        reason = f"confidence {conf_level} < {min_confidence}"

    pos_base = {"HIGH": 0.15, "MEDIUM": 0.10, "LOW": 0.05}[conf_level]
    if mode == "TREND_FALLBACK":
        pos_base *= 0.5
    pos_delta = pos_base if action == "BUY" else (-pos_base if action == "SELL" else 0.0)
    if mode == "TREND_FALLBACK" and conf_level == "LOW" and action == "BUY":
        action = "HOLD"
        pos_delta = 0.0
        reason = "fallback+low confidence"

    return {
        "action": action,
        "buy_price": round(buy_price, 4),
        "sell_price": round(sell_price, 4),
        "stop_price": round(stop_price, 4),
        "position_delta": round(pos_delta, 4),
        "confidence": conf_level,
        "mode": mode,
        "reason": reason,
        "sample_count": sample_count,
    }


def recommend_chan_from_points(
    current_price: Optional[float],
    current_cost: Optional[float],
    points: List[TradePoint],
    allow_small_sample: bool,
    min_confidence: str,
    param_cfg: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    base = recommend_from_points(current_price, current_cost, points, allow_small_sample, min_confidence, param_cfg=param_cfg)
    if current_price is None or current_price <= 0:
        return base

    prices = [p.price for p in points if p.price is not None and p.price > 0]
    n = len(prices)
    if n < 7:
        base["reason"] = "CHAN sample too short; fallback baseline"
        return base

    fractals: List[Tuple[int, str]] = []
    for i in range(1, n - 1):
        if prices[i] > prices[i - 1] and prices[i] > prices[i + 1]:
            fractals.append((i, "TOP"))
        elif prices[i] < prices[i - 1] and prices[i] < prices[i + 1]:
            fractals.append((i, "BOTTOM"))

    pens: List[Tuple[int, int, str]] = []
    for i in range(1, len(fractals)):
        p0, t0 = fractals[i - 1]
        p1, t1 = fractals[i]
        if t0 == t1:
            continue
        direction = "UP" if (t0 == "BOTTOM" and t1 == "TOP") else "DOWN"
        pens.append((p0, p1, direction))

    recent = prices[-6:]
    center = safe_mean(recent)
    spread = max(safe_stdev(recent), 1e-6)
    upper = center + spread * 0.6
    lower = center - spread * 0.6
    trend_up = len(pens) >= 2 and pens[-1][2] == "UP" and pens[-2][2] == "UP"
    trend_down = len(pens) >= 2 and pens[-1][2] == "DOWN" and pens[-2][2] == "DOWN"

    action = "HOLD"
    reason = f"CHAN fractals={len(fractals)} pens={len(pens)} center={center:.4f}"
    if trend_up and current_price <= upper:
        action = "BUY"
        reason = f"{reason}; up-pen continuation near center"
    elif trend_down and current_price >= lower:
        action = "SELL"
        reason = f"{reason}; down-pen continuation near center"

    buy_price = min(base["buy_price"], center) if base["buy_price"] is not None else center
    sell_price = max(base["sell_price"], center) if base["sell_price"] is not None else center
    stop_price = min(base["stop_price"], lower - spread * 0.4) if base["stop_price"] is not None else lower
    conf = base["confidence"]
    if n < 20 and conf == "HIGH":
        conf = "MEDIUM"
    if _level_rank(conf) < _level_rank(min_confidence):
        action = "HOLD"
        reason = f"CHAN confidence {conf} below threshold {min_confidence}"

    pos = base["position_delta"]
    if action == "BUY" and pos < 0:
        pos = abs(pos)
    if action == "SELL" and pos > 0:
        pos = -abs(pos)
    if action == "HOLD":
        pos = 0.0

    return {
        "action": action,
        "buy_price": round(float(buy_price), 4),
        "sell_price": round(float(sell_price), 4),
        "stop_price": round(float(stop_price), 4),
        "position_delta": round(float(pos), 4),
        "confidence": conf,
        "mode": base["mode"],
        "reason": reason,
        "sample_count": base["sample_count"],
    }


def recommend_atr_wave_from_points(
    current_price: Optional[float],
    current_cost: Optional[float],
    points: List[TradePoint],
    allow_small_sample: bool,
    min_confidence: str,
    param_cfg: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    base = recommend_from_points(current_price, current_cost, points, allow_small_sample, min_confidence, param_cfg=param_cfg)
    if current_price is None or current_price <= 0:
        return base

    closes = [p.price for p in points if p.price is not None and p.price > 0]
    if len(closes) < 8:
        base["reason"] = "ATR_WAVE sample too short; fallback baseline"
        return base

    diffs = [abs(closes[i] - closes[i - 1]) for i in range(1, len(closes))]
    atr_window = min(14, len(diffs))
    atr = safe_mean(diffs[-atr_window:]) if atr_window > 0 else 0.0
    ma_window = min(20, len(closes))
    mid = safe_mean(closes[-ma_window:]) if ma_window > 0 else current_price
    upper = mid + atr * 1.2
    lower = mid - atr * 1.2

    action = "HOLD"
    reason = f"ATR_WAVE atr={atr:.4f} mid={mid:.4f} band=[{lower:.4f},{upper:.4f}]"
    if current_price > upper:
        action = "BUY"
        reason = f"{reason}; breakout above upper band"
    elif current_price < lower:
        action = "SELL"
        reason = f"{reason}; breakdown below lower band"
    elif abs(current_price - mid) <= max(atr * 0.25, 0.01):
        action = "HOLD"
        reason = f"{reason}; mean reversion zone"

    buy_price = min(base["buy_price"], current_price - atr * 0.4) if base["buy_price"] is not None else current_price
    sell_price = max(base["sell_price"], current_price + atr * 0.8) if base["sell_price"] is not None else current_price
    stop_price = min(base["stop_price"], current_price - atr * 1.2) if base["stop_price"] is not None else current_price
    conf = base["confidence"]
    if len(closes) < 20 and conf == "HIGH":
        conf = "MEDIUM"
    if _level_rank(conf) < _level_rank(min_confidence):
        action = "HOLD"
        reason = f"ATR_WAVE confidence {conf} below threshold {min_confidence}"

    pos = base["position_delta"]
    if action == "BUY" and pos < 0:
        pos = abs(pos)
    if action == "SELL" and pos > 0:
        pos = -abs(pos)
    if action == "HOLD":
        pos = 0.0

    return {
        "action": action,
        "buy_price": round(float(buy_price), 4),
        "sell_price": round(float(sell_price), 4),
        "stop_price": round(float(stop_price), 4),
        "position_delta": round(float(pos), 4),
        "confidence": conf,
        "mode": base["mode"],
        "reason": reason,
        "sample_count": base["sample_count"],
    }


def _kline_close_returns(bars: List[KBar]) -> List[float]:
    closes = [b.close for b in bars if b.close > 0]
    out: List[float] = []
    for i in range(1, len(closes)):
        prev = closes[i - 1]
        curr = closes[i]
        if prev > 0:
            out.append((curr - prev) / prev)
    return out


def recommend_from_kbars(
    current_price: Optional[float],
    current_cost: Optional[float],
    bars: List[KBar],
    allow_small_sample: bool,
    min_confidence: str,
    param_cfg: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    cfg = param_cfg or {}
    trend_threshold = float(cfg.get("trend_threshold", 0.015))
    vol_cap = float(cfg.get("vol_cap", 0.10))
    band_low = float(cfg.get("band_low", 0.01))
    band_high = float(cfg.get("band_high", 0.12))
    stop_mult = float(cfg.get("stop_mult", 1.8))
    rr_min = float(cfg.get("rr_min", 0.8))
    if current_price is None or current_price <= 0:
        return {
            "action": "HOLD",
            "buy_price": None,
            "sell_price": None,
            "stop_price": None,
            "position_delta": 0.0,
            "confidence": "LOW",
            "mode": "TREND_FALLBACK",
            "reason": "missing current price",
            "sample_count": len(bars),
        }

    valid = [b for b in bars if b.close > 0]
    sample_count = len(valid)
    mode = "FULL_MODEL" if sample_count >= 20 else "TREND_FALLBACK"
    if sample_count < 20 and not allow_small_sample:
        return {
            "action": "HOLD",
            "buy_price": round(current_price, 4),
            "sell_price": round(current_price, 4),
            "stop_price": round(current_price * 0.98, 4),
            "position_delta": 0.0,
            "confidence": "LOW",
            "mode": "TREND_FALLBACK",
            "reason": "sample too small",
            "sample_count": sample_count,
        }

    returns = _kline_close_returns(valid)
    conf_score = _confidence_from_history(sample_count, returns[-30:], [])
    conf_level = _confidence_level(conf_score)
    if mode == "TREND_FALLBACK" and conf_level == "HIGH":
        conf_level = "MEDIUM"

    vol = safe_stdev(returns[-20:]) if returns else 0.02
    if vol > vol_cap:
        return {
            "action": "HOLD",
            "buy_price": round(current_price * 0.99, 4),
            "sell_price": round(current_price * 1.01, 4),
            "stop_price": round(current_price * 0.97, 4),
            "position_delta": 0.0,
            "confidence": conf_level,
            "mode": mode,
            "reason": "volatility too high",
            "sample_count": sample_count,
        }

    closes = [b.close for b in valid]
    ma_window = min(20, len(closes))
    moving_avg = safe_mean(closes[-ma_window:]) if ma_window > 0 else current_price
    trend = (current_price / moving_avg - 1.0) if moving_avg > 0 else 0.0
    band = _clamp(max(vol * 1.2, band_low), band_low, band_high)

    buy_price = current_price * (1.0 - band)
    sell_price = current_price * (1.0 + band)
    stop_price = current_price * (1.0 - band * stop_mult)
    if current_cost is not None and current_cost > 0:
        buy_price = min(buy_price, current_cost * 0.995)
        sell_price = max(sell_price, current_cost * 1.005)

    action = "HOLD"
    reason = "neutral"
    if trend > trend_threshold:
        action = "BUY"
        reason = "up trend"
    elif trend < -trend_threshold:
        action = "SELL"
        reason = "down trend"

    expected_up = max(0.0, (sell_price - current_price) / current_price)
    expected_down = max(0.0, (current_price - stop_price) / current_price)
    if expected_up <= expected_down * rr_min:
        action = "HOLD"
        reason = "risk/reward not favorable"
    if _level_rank(conf_level) < _level_rank(min_confidence):
        action = "HOLD"
        reason = f"confidence {conf_level} < {min_confidence}"

    pos_base = {"HIGH": 0.15, "MEDIUM": 0.10, "LOW": 0.05}[conf_level]
    if mode == "TREND_FALLBACK":
        pos_base *= 0.5
    pos_delta = pos_base if action == "BUY" else (-pos_base if action == "SELL" else 0.0)
    if mode == "TREND_FALLBACK" and conf_level == "LOW" and action == "BUY":
        action = "HOLD"
        pos_delta = 0.0
        reason = "fallback+low confidence"

    return {
        "action": action,
        "buy_price": round(buy_price, 4),
        "sell_price": round(sell_price, 4),
        "stop_price": round(stop_price, 4),
        "position_delta": round(pos_delta, 4),
        "confidence": conf_level,
        "mode": mode,
        "reason": reason,
        "sample_count": sample_count,
    }


def recommend_chan_from_kbars(
    current_price: Optional[float],
    current_cost: Optional[float],
    bars: List[KBar],
    allow_small_sample: bool,
    min_confidence: str,
    param_cfg: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    base = recommend_from_kbars(current_price, current_cost, bars, allow_small_sample, min_confidence, param_cfg=param_cfg)
    if current_price is None or current_price <= 0:
        return base
    if len(bars) < 7:
        base["reason"] = "CHAN sample too short; fallback baseline"
        return base

    fractals: List[Tuple[int, str]] = []
    for i in range(1, len(bars) - 1):
        if bars[i].high > bars[i - 1].high and bars[i].high > bars[i + 1].high:
            fractals.append((i, "TOP"))
        elif bars[i].low < bars[i - 1].low and bars[i].low < bars[i + 1].low:
            fractals.append((i, "BOTTOM"))

    pens: List[Tuple[int, int, str]] = []
    for i in range(1, len(fractals)):
        p0, t0 = fractals[i - 1]
        p1, t1 = fractals[i]
        if t0 == t1:
            continue
        pens.append((p0, p1, "UP" if (t0 == "BOTTOM" and t1 == "TOP") else "DOWN"))

    recent = bars[-6:]
    center = safe_mean([b.close for b in recent])
    spread = max(safe_stdev([b.close for b in recent]), 1e-6)
    upper = center + spread * 0.6
    lower = center - spread * 0.6
    trend_up = len(pens) >= 2 and pens[-1][2] == "UP" and pens[-2][2] == "UP"
    trend_down = len(pens) >= 2 and pens[-1][2] == "DOWN" and pens[-2][2] == "DOWN"

    action = "HOLD"
    reason = f"CHAN fractals={len(fractals)} pens={len(pens)} center={center:.4f}"
    if trend_up and current_price <= upper:
        action = "BUY"
        reason = f"{reason}; up-pen continuation near center"
    elif trend_down and current_price >= lower:
        action = "SELL"
        reason = f"{reason}; down-pen continuation near center"

    buy_price = min(base["buy_price"], center) if base["buy_price"] is not None else center
    sell_price = max(base["sell_price"], center) if base["sell_price"] is not None else center
    stop_price = min(base["stop_price"], lower - spread * 0.4) if base["stop_price"] is not None else lower
    conf = base["confidence"]
    if len(bars) < 20 and conf == "HIGH":
        conf = "MEDIUM"
    if _level_rank(conf) < _level_rank(min_confidence):
        action = "HOLD"
        reason = f"CHAN confidence {conf} below threshold {min_confidence}"

    pos = base["position_delta"]
    if action == "BUY" and pos < 0:
        pos = abs(pos)
    if action == "SELL" and pos > 0:
        pos = -abs(pos)
    if action == "HOLD":
        pos = 0.0

    return {
        "action": action,
        "buy_price": round(float(buy_price), 4),
        "sell_price": round(float(sell_price), 4),
        "stop_price": round(float(stop_price), 4),
        "position_delta": round(float(pos), 4),
        "confidence": conf,
        "mode": base["mode"],
        "reason": reason,
        "sample_count": base["sample_count"],
    }


def recommend_atr_wave_from_kbars(
    current_price: Optional[float],
    current_cost: Optional[float],
    bars: List[KBar],
    allow_small_sample: bool,
    min_confidence: str,
    param_cfg: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    base = recommend_from_kbars(current_price, current_cost, bars, allow_small_sample, min_confidence, param_cfg=param_cfg)
    if current_price is None or current_price <= 0:
        return base
    if len(bars) < 8:
        base["reason"] = "ATR_WAVE sample too short; fallback baseline"
        return base

    tr_values: List[float] = []
    prev_close = bars[0].close
    for bar in bars[1:]:
        tr = max(bar.high - bar.low, abs(bar.high - prev_close), abs(bar.low - prev_close))
        tr_values.append(max(tr, 0.0))
        prev_close = bar.close
    atr_window = min(14, len(tr_values))
    atr = safe_mean(tr_values[-atr_window:]) if atr_window > 0 else 0.0
    closes = [b.close for b in bars]
    mid = safe_mean(closes[-min(20, len(closes)) :]) if closes else current_price
    upper = mid + atr * 1.2
    lower = mid - atr * 1.2

    action = "HOLD"
    reason = f"ATR_WAVE atr={atr:.4f} mid={mid:.4f} band=[{lower:.4f},{upper:.4f}]"
    if current_price > upper:
        action = "BUY"
        reason = f"{reason}; breakout above upper band"
    elif current_price < lower:
        action = "SELL"
        reason = f"{reason}; breakdown below lower band"
    elif abs(current_price - mid) <= max(atr * 0.25, 0.01):
        action = "HOLD"
        reason = f"{reason}; mean reversion zone"

    buy_price = min(base["buy_price"], current_price - atr * 0.4) if base["buy_price"] is not None else current_price
    sell_price = max(base["sell_price"], current_price + atr * 0.8) if base["sell_price"] is not None else current_price
    stop_price = min(base["stop_price"], current_price - atr * 1.2) if base["stop_price"] is not None else current_price
    conf = base["confidence"]
    if len(closes) < 20 and conf == "HIGH":
        conf = "MEDIUM"
    if _level_rank(conf) < _level_rank(min_confidence):
        action = "HOLD"
        reason = f"ATR_WAVE confidence {conf} below threshold {min_confidence}"

    pos = base["position_delta"]
    if action == "BUY" and pos < 0:
        pos = abs(pos)
    if action == "SELL" and pos > 0:
        pos = -abs(pos)
    if action == "HOLD":
        pos = 0.0

    return {
        "action": action,
        "buy_price": round(float(buy_price), 4),
        "sell_price": round(float(sell_price), 4),
        "stop_price": round(float(stop_price), 4),
        "position_delta": round(float(pos), 4),
        "confidence": conf,
        "mode": base["mode"],
        "reason": reason,
        "sample_count": base["sample_count"],
    }


def recommend_by_strategy_kline(
    strategy_id: str,
    current_price: Optional[float],
    current_cost: Optional[float],
    bars: List[KBar],
    allow_small_sample: bool,
    min_confidence: str,
    param_cfg: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    req = StrategyEvalRequest(
        strategy_id=strategy_id,
        source="kline",
        current_price=current_price,
        current_cost=current_cost,
        allow_small_sample=allow_small_sample,
        min_confidence=min_confidence,
        param_cfg=param_cfg,
        bars=bars,
    )
    return get_strategy_registry().evaluate(req)


def recommend_by_strategy_trade(
    strategy_id: str,
    current_price: Optional[float],
    current_cost: Optional[float],
    points: List[TradePoint],
    allow_small_sample: bool,
    min_confidence: str,
    param_cfg: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    req = StrategyEvalRequest(
        strategy_id=strategy_id,
        source="trade",
        current_price=current_price,
        current_cost=current_cost,
        allow_small_sample=allow_small_sample,
        min_confidence=min_confidence,
        param_cfg=param_cfg,
        points=points,
    )
    return get_strategy_registry().evaluate(req)


def _eval_baseline(req: StrategyEvalRequest) -> Dict[str, Any]:
    if req.source == "trade":
        return recommend_from_points(
            current_price=req.current_price,
            current_cost=req.current_cost,
            points=req.points,
            allow_small_sample=req.allow_small_sample,
            min_confidence=req.min_confidence,
            param_cfg=req.param_cfg,
        )
    return recommend_from_kbars(
        current_price=req.current_price,
        current_cost=req.current_cost,
        bars=req.bars,
        allow_small_sample=req.allow_small_sample,
        min_confidence=req.min_confidence,
        param_cfg=req.param_cfg,
    )


def _eval_chan(req: StrategyEvalRequest) -> Dict[str, Any]:
    if req.source == "trade":
        return recommend_chan_from_points(
            current_price=req.current_price,
            current_cost=req.current_cost,
            points=req.points,
            allow_small_sample=req.allow_small_sample,
            min_confidence=req.min_confidence,
            param_cfg=req.param_cfg,
        )
    return recommend_chan_from_kbars(
        current_price=req.current_price,
        current_cost=req.current_cost,
        bars=req.bars,
        allow_small_sample=req.allow_small_sample,
        min_confidence=req.min_confidence,
        param_cfg=req.param_cfg,
    )


def _eval_atr_wave(req: StrategyEvalRequest) -> Dict[str, Any]:
    if req.source == "trade":
        return recommend_atr_wave_from_points(
            current_price=req.current_price,
            current_cost=req.current_cost,
            points=req.points,
            allow_small_sample=req.allow_small_sample,
            min_confidence=req.min_confidence,
            param_cfg=req.param_cfg,
        )
    return recommend_atr_wave_from_kbars(
        current_price=req.current_price,
        current_cost=req.current_cost,
        bars=req.bars,
        allow_small_sample=req.allow_small_sample,
        min_confidence=req.min_confidence,
        param_cfg=req.param_cfg,
    )


def get_registered_strategy_ids() -> List[str]:
    return get_strategy_ids()


register_strategy("baseline", _eval_baseline)
register_strategy("chan", _eval_chan)
register_strategy("atr_wave", _eval_atr_wave)


__all__ = [
    "recommend_from_points",
    "recommend_chan_from_points",
    "recommend_atr_wave_from_points",
    "recommend_by_strategy_trade",
    "recommend_from_kbars",
    "recommend_chan_from_kbars",
    "recommend_atr_wave_from_kbars",
    "recommend_by_strategy_kline",
    "get_registered_strategy_ids",
]
