"""
SignalEdge Bot v5.0 — Triple Engine
====================================
🏦 Institutional Engine: Multi-Timeframe Order Block Scanner
                         (Scalping · Day · Swing · Position)
🤖 AI Engine:            Real-time Classic TA
                         (RSI · MACD · Volume · Breakouts · S/R)
📊 Market Scanner:       Real BUY/SELL/HOLD tags for all pairs

Scans 200 pairs in parallel every ~60 seconds.
Auto-fallback across exchanges if geo-blocked.

Author:  SignalEdge
Version: 5.0.0
"""

import os
import time
import logging
import requests
import traceback
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
log = logging.getLogger("SignalEdge")

# ═══════════════════════════════════════════════════
# CONFIG
# ═══════════════════════════════════════════════════
WEBHOOK_URL    = os.environ.get("WEBHOOK_URL",    "https://signaledge-server.onrender.com/webhook")
AI_WEBHOOK_URL = os.environ.get("AI_WEBHOOK_URL", "https://signaledge-server.onrender.com/webhook-ai")
SCAN_WEBHOOK_URL = os.environ.get("SCAN_WEBHOOK_URL", "https://signaledge-server.onrender.com/webhook-scan")
WEBHOOK_SECRET = os.environ.get("WEBHOOK_SECRET", "signaledge2025")
SCAN_INTERVAL  = int(os.environ.get("SCAN_INTERVAL_MINUTES", "1")) * 60
MAX_WORKERS    = int(os.environ.get("MAX_WORKERS", "20"))

# Institutional (Order Block) config
OB_LOOKBACK      = 6
SL_BUFFER_PCT    = 0.002
FIB1             = 1.618
FIB2             = 2.0
MIN_BODY_RATIO   = 0.30
MIN_RR           = 1.0
COOLDOWN_HRS     = 2

# AI Signals config
AI_MIN_CONFIDENCE = 65
AI_COOLDOWN_MIN   = 30
RSI_OVERSOLD      = 30
RSI_OVERBOUGHT    = 70
VOL_SPIKE_RATIO   = 2.0

# ═══════════════════════════════════════════════════
# TRADING STYLES
# ═══════════════════════════════════════════════════
STYLES = {
    "scalp":    {"label":"Scalping",       "htf":"15m", "candles":150, "emoji":"⚡", "hold":"Minutes to 2 hours", "pairs":50},
    "day":      {"label":"Day Trade",      "htf":"1h",  "candles":150, "emoji":"📊", "hold":"2–24 hours",        "pairs":100},
    "swing":    {"label":"Swing Trade",    "htf":"4h",  "candles":150, "emoji":"🎯", "hold":"1–7 days",          "pairs":200},
    "position": {"label":"Position Trade", "htf":"1d",  "candles":150, "emoji":"🏦", "hold":"1–4 weeks",         "pairs":100},
}

# ═══════════════════════════════════════════════════
# TOP 200 CRYPTO PAIRS
# ═══════════════════════════════════════════════════
PAIRS = [
    "BTC/USDT","ETH/USDT","BNB/USDT","SOL/USDT","XRP/USDT",
    "DOGE/USDT","ADA/USDT","AVAX/USDT","SHIB/USDT","TRX/USDT",
    "DOT/USDT","LINK/USDT","MATIC/USDT","TON/USDT","UNI/USDT",
    "LTC/USDT","BCH/USDT","NEAR/USDT","ICP/USDT","APT/USDT",
    "FIL/USDT","ARB/USDT","OP/USDT","INJ/USDT","ATOM/USDT",
    "VET/USDT","GRT/USDT","ALGO/USDT","EGLD/USDT","XLM/USDT",
    "MANA/USDT","SAND/USDT","AXS/USDT","ENJ/USDT","THETA/USDT",
    "FLOW/USDT","XTZ/USDT","EOS/USDT","NEO/USDT","KAVA/USDT",
    "ZIL/USDT","ONE/USDT","CELO/USDT","ROSE/USDT","ANKR/USDT",
    "SUI/USDT","SEI/USDT","TIA/USDT","STX/USDT","RNDR/USDT",
    "FET/USDT","AGIX/USDT","OCEAN/USDT","WLD/USDT","CFX/USDT",
    "BLUR/USDT","DYDX/USDT","ENS/USDT","FLOKI/USDT","GALA/USDT",
    "GMT/USDT","HBAR/USDT","HOT/USDT","IMX/USDT","JASMY/USDT",
    "KSM/USDT","LRC/USDT","MAGIC/USDT","MASK/USDT","MINA/USDT",
    "NEXO/USDT","ONT/USDT","PEPE/USDT","QNT/USDT","QTUM/USDT",
    "RAY/USDT","RUNE/USDT","RVN/USDT","SNX/USDT","STORJ/USDT",
    "SUPER/USDT","TFUEL/USDT","TWT/USDT","UNFI/USDT","VGX/USDT",
    "WAVES/USDT","WIN/USDT","WOO/USDT","XMR/USDT","YFI/USDT",
    "ZEC/USDT","ZRX/USDT","1INCH/USDT","AAVE/USDT","ACH/USDT",
    "ALPHA/USDT","ARPA/USDT","BAL/USDT","BAKE/USDT","BNT/USDT",
    "BSW/USDT","C98/USDT","CAKE/USDT","CHR/USDT","CHZ/USDT",
    "CLV/USDT","COMP/USDT","COTI/USDT","CRV/USDT","CVC/USDT",
    "CVX/USDT","DAR/USDT","DENT/USDT","DGB/USDT","ELF/USDT",
    "ERN/USDT","FARM/USDT","FTM/USDT","FUN/USDT","GHST/USDT",
    "GLM/USDT","GLMR/USDT","GNO/USDT","HIFI/USDT","ILV/USDT",
    "IOST/USDT","IOTX/USDT","LIT/USDT","LOKA/USDT","LOOM/USDT",
    "LPT/USDT","MDT/USDT","MTL/USDT","NKN/USDT","OGN/USDT",
    "OMG/USDT","PEOPLE/USDT","REQ/USDT","RLC/USDT","SC/USDT",
    "SPELL/USDT","SXP/USDT","SYS/USDT","TOMO/USDT","UFT/USDT",
    "UTK/USDT","VOXEL/USDT","WAN/USDT","XEM/USDT","XNO/USDT",
    "XVG/USDT","YGG/USDT","ZEN/USDT","ACM/USDT","AERGO/USDT",
    "AGLD/USDT","AKRO/USDT","AMP/USDT","ASR/USDT","AUTO/USDT",
    "AVA/USDT","BADGER/USDT","BAND/USDT","BETA/USDT","BLZ/USDT",
    "CKB/USDT","CTSI/USDT","CVP/USDT","DATA/USDT","DOCK/USDT",
    "DREP/USDT","DUSK/USDT","EDO/USDT","EPIK/USDT","FOR/USDT",
    "FORTH/USDT","FRONT/USDT","HARD/USDT","IRIS/USDT","LAZIO/USDT",
    "LINA/USDT","LUNA/USDT","NULS/USDT","OG/USDT","REEF/USDT",
    "REN/USDT","SKL/USDT","ALCX/USDT","AUCTION/USDT","CREAM/USDT",
    "CONV/USDT","COS/USDT","HIGH/USDT","ID/USDT","NMR/USDT",
    "POLS/USDT","PERP/USDT","QUICK/USDT","RARE/USDT","SAFEMOON/USDT",
]

# ═══════════════════════════════════════════════════
# DEDUPLICATION
# ═══════════════════════════════════════════════════
recent_signals: dict = {}
recent_ai_signals: dict = {}

def already_signalled(symbol: str, style: str, direction: str) -> bool:
    key = f"{symbol}_{style}_{direction}"
    if key in recent_signals:
        elapsed = (datetime.now(timezone.utc) - recent_signals[key]).total_seconds()
        if elapsed < COOLDOWN_HRS * 3600:
            return True
    return False

def mark_signalled(symbol: str, style: str, direction: str):
    recent_signals[f"{symbol}_{style}_{direction}"] = datetime.now(timezone.utc)

def already_ai_signalled(symbol: str, direction: str) -> bool:
    key = f"{symbol}_{direction}"
    if key in recent_ai_signals:
        elapsed = (datetime.now(timezone.utc) - recent_ai_signals[key]).total_seconds()
        if elapsed < AI_COOLDOWN_MIN * 60:
            return True
    return False

def mark_ai_signalled(symbol: str, direction: str):
    recent_ai_signals[f"{symbol}_{direction}"] = datetime.now(timezone.utc)

# ═══════════════════════════════════════════════════
# TA INDICATORS (pure python, no pandas needed)
# ═══════════════════════════════════════════════════
def calc_rsi(closes: list, period: int = 14) -> float:
    if len(closes) < period + 1:
        return 50.0
    gains = []
    losses = []
    for i in range(1, period + 1):
        diff = closes[-i] - closes[-i-1]
        if diff > 0:
            gains.append(diff)
            losses.append(0)
        else:
            gains.append(0)
            losses.append(abs(diff))
    avg_gain = sum(gains) / period
    avg_loss = sum(losses) / period
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return round(100 - (100 / (1 + rs)), 2)

def calc_ema(values: list, period: int) -> float:
    if len(values) < period:
        return sum(values) / len(values) if values else 0
    multiplier = 2 / (period + 1)
    ema = sum(values[:period]) / period
    for price in values[period:]:
        ema = (price - ema) * multiplier + ema
    return ema

def calc_macd(closes: list) -> tuple:
    """Returns (macd_line, signal_line, histogram)"""
    if len(closes) < 35:
        return 0, 0, 0
    ema12 = calc_ema(closes, 12)
    ema26 = calc_ema(closes, 26)
    macd_line = ema12 - ema26
    # Signal line is 9-period EMA of MACD — approximate
    macd_values = []
    for i in range(26, len(closes)):
        e12 = calc_ema(closes[:i+1], 12)
        e26 = calc_ema(closes[:i+1], 26)
        macd_values.append(e12 - e26)
    signal_line = calc_ema(macd_values, 9) if len(macd_values) >= 9 else 0
    histogram = macd_line - signal_line
    return round(macd_line, 6), round(signal_line, 6), round(histogram, 6)

def calc_volume_spike(volumes: list, lookback: int = 20) -> float:
    """Returns ratio of current volume to average. 2.0 = 2x avg = spike"""
    if len(volumes) < lookback + 1:
        return 1.0
    current = volumes[-1]
    avg = sum(volumes[-lookback-1:-1]) / lookback
    if avg == 0:
        return 1.0
    return round(current / avg, 2)

def detect_breakout(candles: list, lookback: int = 20) -> str:
    """Returns 'UP', 'DOWN', or '' if no breakout"""
    if len(candles) < lookback + 1:
        return ''
    recent_highs = [c[2] for c in candles[-lookback-1:-1]]
    recent_lows  = [c[3] for c in candles[-lookback-1:-1]]
    last_close = candles[-1][4]
    max_high = max(recent_highs)
    min_low  = min(recent_lows)
    if last_close > max_high * 1.002:  # 0.2% above resistance
        return 'UP'
    if last_close < min_low * 0.998:   # 0.2% below support
        return 'DOWN'
    return ''

def detect_sr_bounce(candles: list, lookback: int = 20) -> str:
    """Detect bounce off support/resistance. Returns 'SUPPORT', 'RESISTANCE', or ''"""
    if len(candles) < lookback + 2:
        return ''
    recent_highs = [c[2] for c in candles[-lookback-2:-2]]
    recent_lows  = [c[3] for c in candles[-lookback-2:-2]]
    last_low    = candles[-1][3]
    last_close  = candles[-1][4]
    last_open   = candles[-1][1]
    min_low = min(recent_lows)
    max_high = max(recent_highs)
    # Support bounce (bullish)
    if abs(last_low - min_low) / min_low < 0.005 and last_close > last_open:
        return 'SUPPORT'
    # Resistance rejection (bearish)
    last_high = candles[-1][2]
    if abs(last_high - max_high) / max_high < 0.005 and last_close < last_open:
        return 'RESISTANCE'
    return ''

# ═══════════════════════════════════════════════════
# AI SIGNAL ENGINE
# ═══════════════════════════════════════════════════
# MARKET TAG (lightweight per-coin signal)
# ═══════════════════════════════════════════════════
def compute_market_tag(candles: list) -> dict:
    """
    Computes a lightweight BUY/SELL/HOLD tag for any coin based on RSI, MACD, volume.
    Returns: {signal, rsi, vol_surge, strength}
    """
    if len(candles) < 30:
        return {'signal':'hold', 'rsi':50, 'vol_surge':False, 'strength':0}

    closes  = [c[4] for c in candles]
    volumes = [c[5] for c in candles]

    rsi = calc_rsi(closes, 14)
    macd, sig, hist = calc_macd(closes)
    vol_ratio = calc_volume_spike(volumes)

    bull, bear = 0, 0

    # RSI component
    if rsi <= 30:   bull += 35
    elif rsi <= 40: bull += 15
    elif rsi >= 70: bear += 35
    elif rsi >= 60: bear += 15

    # MACD component
    if hist > 0 and macd > sig: bull += 30
    elif hist < 0 and macd < sig: bear += 30

    # Volume directional
    vol_surge = vol_ratio >= 1.5
    if vol_surge and len(candles) >= 2:
        last = candles[-1]
        if last[4] > last[1]: bull += 15
        else: bear += 15

    # Momentum (5-candle direction)
    if len(closes) >= 5:
        recent_change = (closes[-1] - closes[-5]) / closes[-5] * 100 if closes[-5] else 0
        if recent_change > 2:   bull += 15
        elif recent_change < -2: bear += 15

    if bull >= 50 and bull > bear + 15:
        signal = 'buy'
        strength = min(bull, 95)
    elif bear >= 50 and bear > bull + 15:
        signal = 'sell'
        strength = min(bear, 95)
    else:
        signal = 'hold'
        strength = max(bull, bear)

    return {
        'signal':    signal,
        'rsi':       round(rsi, 1),
        'vol_surge': vol_surge,
        'vol_ratio': round(vol_ratio, 2),
        'strength':  int(strength)
    }

# ═══════════════════════════════════════════════════
# AI SIGNAL ANALYSIS (RSI + MACD + Volume + Breakout)
# ═══════════════════════════════════════════════════
def analyze_ai(symbol: str, candles: list) -> dict | None:
    """
    Combines RSI + MACD + Volume + Breakout + S/R bounce.
    Returns signal if >= 2 indicators align with >= AI_MIN_CONFIDENCE confidence.
    """
    if len(candles) < 50:
        return None
    closes  = [c[4] for c in candles]
    volumes = [c[5] for c in candles]

    # Indicators
    rsi = calc_rsi(closes, 14)
    macd, sig, hist = calc_macd(closes)
    vol_ratio = calc_volume_spike(volumes)
    breakout  = detect_breakout(candles)
    sr        = detect_sr_bounce(candles)

    # Scoring — each signal contributes to bullish or bearish score (0-100)
    bull_score = 0
    bear_score = 0
    tags = []

    # RSI
    if rsi <= RSI_OVERSOLD:
        bull_score += 30; tags.append('RSI')
    elif rsi <= 40:
        bull_score += 15
    elif rsi >= RSI_OVERBOUGHT:
        bear_score += 30; tags.append('RSI OB')
    elif rsi >= 60:
        bear_score += 15

    # MACD
    if hist > 0 and macd > sig:
        bull_score += 25; tags.append('MACD')
    elif hist < 0 and macd < sig:
        bear_score += 25; tags.append('MACD')

    # Volume spike (directional by candle color)
    if vol_ratio >= VOL_SPIKE_RATIO:
        last_c = candles[-1]
        if last_c[4] > last_c[1]:
            bull_score += 20; tags.append('VOL')
        else:
            bear_score += 20; tags.append('VOL')

    # Breakout
    if breakout == 'UP':
        bull_score += 25; tags.append('BREAKOUT')
    elif breakout == 'DOWN':
        bear_score += 25; tags.append('BREAKDOWN')

    # S/R bounce
    if sr == 'SUPPORT':
        bull_score += 20; tags.append('SUPPORT')
    elif sr == 'RESISTANCE':
        bear_score += 20; tags.append('RESISTANCE')

    # Decide direction
    if bull_score >= AI_MIN_CONFIDENCE and bull_score > bear_score:
        direction = 'BUY'; confidence = min(bull_score, 95)
    elif bear_score >= AI_MIN_CONFIDENCE and bear_score > bull_score:
        direction = 'SELL'; confidence = min(bear_score, 95)
    else:
        return None

    # Require at least 2 tags (2+ indicators aligned)
    if len(tags) < 2:
        return None

    last_c = candles[-1]
    price = last_c[4]

    # Build reasoning summary
    reason_parts = []
    if rsi <= RSI_OVERSOLD: reason_parts.append(f"RSI oversold at {rsi}")
    elif rsi >= RSI_OVERBOUGHT: reason_parts.append(f"RSI overbought at {rsi}")
    if 'MACD' in tags: reason_parts.append("MACD " + ("bullish crossover" if direction=='BUY' else "bearish crossover"))
    if 'VOL' in tags: reason_parts.append(f"Volume {vol_ratio}x average")
    if breakout == 'UP': reason_parts.append("Resistance breakout")
    if breakout == 'DOWN': reason_parts.append("Support breakdown")
    if sr == 'SUPPORT': reason_parts.append("Support bounce")
    if sr == 'RESISTANCE': reason_parts.append("Resistance rejection")

    reason = '. '.join(reason_parts[:3]) + '.'

    # ATR-based SL/TP (14-candle Average True Range)
    atr = 0.0
    if len(candles) >= 15:
        trs = []
        for i in range(-14, 0):
            hi, lo, prev_close = candles[i][2], candles[i][3], candles[i-1][4]
            tr = max(hi - lo, abs(hi - prev_close), abs(lo - prev_close))
            trs.append(tr)
        atr = sum(trs) / len(trs) if trs else 0.0

    # Safety fallback if ATR is 0
    if atr <= 0:
        atr = price * 0.015  # 1.5% default volatility

    # SL = 1.5× ATR, TP1 = 1.5× ATR, TP2 = 3× ATR (1:1 and 1:2 RR)
    if direction == 'BUY':
        sl  = round(price - 1.5 * atr, 8)
        tp1 = round(price + 1.5 * atr, 8)
        tp2 = round(price + 3.0 * atr, 8)
    else:
        sl  = round(price + 1.5 * atr, 8)
        tp1 = round(price - 1.5 * atr, 8)
        tp2 = round(price - 3.0 * atr, 8)

    return {
        'symbol':     symbol.replace('/USDT', ''),
        'type':       direction.lower(),
        'price':      price,
        'entry':      price,
        'sl':         sl,
        'tp1':        tp1,
        'tp2':        tp2,
        'confidence': confidence,
        'tags':       tags[:3],
        'reason':     reason,
        'rsi':        rsi,
        'timeframe':  '1h'
    }

# ═══════════════════════════════════════════════════
# INSTITUTIONAL (ORDER BLOCK) ENGINE
# ═══════════════════════════════════════════════════
def detect_order_block(candles: list) -> dict:
    if len(candles) < OB_LOOKBACK + 5:
        return {}
    last = candles[-1]
    prev = candles[-2]
    o, h, l, c = last[1], last[2], last[3], last[4]
    body = abs(c - o)
    rng  = h - l
    if rng == 0 or body / rng < MIN_BODY_RATIO:
        return {}
    ob = {}
    if c > prev[2] and c > o:
        for i in range(2, OB_LOOKBACK + 2):
            if i >= len(candles): break
            cd = candles[-i]
            if cd[4] < cd[1]:
                ob['bull_ob'] = {'high': cd[2], 'low': cd[3]}
                break
    if c < prev[3] and c < o:
        for i in range(2, OB_LOOKBACK + 2):
            if i >= len(candles): break
            cd = candles[-i]
            if cd[4] > cd[1]:
                ob['bear_ob'] = {'high': cd[2], 'low': cd[3]}
                break
    return ob

def detect_entry(candles: list) -> dict | None:
    if len(candles) < 30:
        return None
    ob = detect_order_block(candles)
    if not ob:
        return None
    last_c = candles[-1][4]
    last_h = candles[-1][2]
    last_l = candles[-1][3]
    prev_c = candles[-2][4]
    if 'bear_ob' in ob:
        ob_h = ob['bear_ob']['high']
        ob_l = ob['bear_ob']['low']
        if (prev_c <= ob_h and last_c > ob_h) or \
           (last_l <= ob_h and last_c > ob_h and prev_c > ob_h):
            return {'type': 'BUY', 'entry': last_c, 'ob_high': ob_h, 'ob_low': ob_l}
    if 'bull_ob' in ob:
        ob_h = ob['bull_ob']['high']
        ob_l = ob['bull_ob']['low']
        if (prev_c >= ob_l and last_c < ob_l) or \
           (last_h >= ob_l and last_c < ob_l and prev_c < ob_l):
            return {'type': 'SELL', 'entry': last_c, 'ob_high': ob_h, 'ob_low': ob_l}
    return None

def calculate_levels(entry: float, ob_high: float, ob_low: float, sig_type: str) -> dict:
    if sig_type == 'BUY':
        sl   = ob_low  * (1 - SL_BUFFER_PCT)
        risk = entry - sl
        tp1  = entry + risk
        tp2  = entry + risk * FIB1
        tp3  = entry + risk * FIB2
    else:
        sl   = ob_high * (1 + SL_BUFFER_PCT)
        risk = sl - entry
        tp1  = entry - risk
        tp2  = entry - risk * FIB1
        tp3  = entry - risk * FIB2
    rr = round(abs(tp1 - entry) / abs(entry - sl), 2) if abs(entry - sl) > 0 else 0
    return {
        'sl':       round(sl, 8),
        'tp1':      round(tp1, 8),
        'tp2':      round(tp2, 8),
        'tp3':      round(tp3, 8),
        'risk_pct': round(abs(entry - sl) / entry * 100, 2),
        'rr':       rr
    }

# ═══════════════════════════════════════════════════
# WEBHOOK SENDERS
# ═══════════════════════════════════════════════════
def send_institutional_signal(symbol: str, sig_type: str, entry: float, levels: dict,
                              style: str, style_cfg: dict, timeframe: str) -> bool:
    payload = {
        'secret':    WEBHOOK_SECRET,
        'type':      sig_type,
        'symbol':    symbol.replace('/USDT', 'USDT'),
        'price':     entry,
        'sl':        levels['sl'],
        'tp1':       levels['tp1'],
        'tp2':       levels['tp2'],
        'tp3':       levels['tp3'],
        'timeframe': timeframe,
        'strategy':  'SignalEdge Institutional',
        'style':     style,
        'style_label': style_cfg['label'],
        'hold':      style_cfg['hold'],
        'risk_pct':  levels['risk_pct'],
        'rr':        levels['rr'],
        'timestamp': datetime.now(timezone.utc).isoformat()
    }
    try:
        r = requests.post(WEBHOOK_URL, json=payload, timeout=8)
        if r.status_code == 200:
            return True
        log.warning(f"  ⚠️ Webhook {r.status_code} for {symbol} [{style}]")
    except Exception as e:
        log.error(f"  ❌ Webhook error for {symbol}: {e}")
    return False

def send_ai_signal(signal: dict) -> bool:
    payload = {
        'secret':     WEBHOOK_SECRET,
        'symbol':     signal['symbol'],
        'type':       signal['type'],
        'price':      signal['price'],
        'entry':      signal.get('entry', signal['price']),
        'sl':         signal.get('sl', 0),
        'tp1':        signal.get('tp1', 0),
        'tp2':        signal.get('tp2', 0),
        'confidence': signal['confidence'],
        'tags':       signal['tags'],
        'reason':     signal['reason'],
        'rsi':        signal['rsi'],
        'timeframe':  signal['timeframe'],
        'timestamp':  datetime.now(timezone.utc).isoformat()
    }
    try:
        r = requests.post(AI_WEBHOOK_URL, json=payload, timeout=8)
        if r.status_code == 200:
            return True
        log.warning(f"  ⚠️ AI webhook {r.status_code} for {signal['symbol']}")
    except Exception as e:
        log.error(f"  ❌ AI webhook error for {signal['symbol']}: {e}")
    return False

# ═══════════════════════════════════════════════════
# OHLCV FETCH WITH RETRY
# ═══════════════════════════════════════════════════
def fetch_ohlcv(exchange, symbol: str, timeframe: str, limit: int = 150):
    for attempt in range(3):
        try:
            return exchange.fetch_ohlcv(symbol, timeframe, limit=limit)
        except Exception as e:
            if attempt == 2:
                return None
            time.sleep(0.5)
    return None

# ═══════════════════════════════════════════════════
# PARALLEL SCAN — AI Signals (all pairs on 1h)
# ═══════════════════════════════════════════════════
def scan_ai_single(exchange, symbol: str) -> dict:
    """
    Returns {tag: {...}, signal: {...} or None, symbol: 'BTC'}
    tag = lightweight BUY/SELL/HOLD (always present if candles fetch works)
    signal = full AI signal (only if threshold met)
    """
    result = {'symbol': symbol.replace('/USDT', '').replace('/USD', ''), 'tag': None, 'signal': None}
    candles = fetch_ohlcv(exchange, symbol, '1h', 100)
    if not candles or len(candles) < 30:
        return result

    # Always compute lightweight tag
    result['tag'] = compute_market_tag(candles)

    # Check for full AI signal (needs 50+ candles)
    if len(candles) >= 50:
        sig = analyze_ai(symbol, candles)
        if sig and not already_ai_signalled(sig['symbol'], sig['type']):
            result['signal'] = sig
    return result

def send_market_scan(coins_dict: dict) -> bool:
    """Send batched BUY/SELL/HOLD tags for all scanned coins to server."""
    if not coins_dict:
        return False
    payload = {'secret': WEBHOOK_SECRET, 'coins': coins_dict}
    try:
        r = requests.post(SCAN_WEBHOOK_URL, json=payload, timeout=10)
        if r.status_code == 200:
            return True
        log.warning(f"⚠️ Market scan webhook {r.status_code}")
    except Exception as e:
        log.error(f"❌ Market scan webhook error: {e}")
    return False

def run_ai_scan(exchange, valid_pairs: list) -> int:
    log.info("🤖 [AI Signals] Scanning " + str(len(valid_pairs)) + " pairs on 1h (parallel)...")
    fired = 0
    market_tags = {}  # NEW: collect lightweight tags for all pairs
    start = time.time()
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {pool.submit(scan_ai_single, exchange, p): p for p in valid_pairs}
        for fut in as_completed(futures):
            try:
                res = fut.result()
                if not res:
                    continue
                # Collect market tag for every coin that returned data
                if res.get('tag') and res.get('symbol'):
                    market_tags[res['symbol']] = res['tag']
                # Fire full AI signal if threshold met
                sig = res.get('signal')
                if sig:
                    if send_ai_signal(sig):
                        mark_ai_signalled(sig['symbol'], sig['type'])
                        log.info(f"  🤖 {sig['symbol']} {sig['type'].upper()} @ {sig['price']} | conf:{sig['confidence']}% | tags:{','.join(sig['tags'])}")
                        fired += 1
            except Exception as e:
                pass
    elapsed = round(time.time() - start, 1)

    # Upload batched market tags
    if market_tags:
        ok = send_market_scan(market_tags)
        buy_n  = sum(1 for t in market_tags.values() if t['signal'] == 'buy')
        sell_n = sum(1 for t in market_tags.values() if t['signal'] == 'sell')
        hold_n = sum(1 for t in market_tags.values() if t['signal'] == 'hold')
        log.info(f"📊 Market scan: {len(market_tags)} coins → Buy:{buy_n} · Sell:{sell_n} · Hold:{hold_n} → upload {'✅' if ok else '❌'}")

    log.info(f"🤖 [AI Signals] Done — {fired} signals in {elapsed}s")
    return fired

# ═══════════════════════════════════════════════════
# PARALLEL SCAN — Institutional (by style)
# ═══════════════════════════════════════════════════
def scan_inst_single(exchange, symbol: str, htf: str, candles_limit: int) -> dict | None:
    candles = fetch_ohlcv(exchange, symbol, htf, candles_limit)
    if not candles:
        return None
    entry = detect_entry(candles)
    if not entry:
        return None
    levels = calculate_levels(entry['entry'], entry['ob_high'], entry['ob_low'], entry['type'])
    if levels['rr'] < MIN_RR:
        return None
    return {
        'symbol':    symbol,
        'sig_type':  entry['type'],
        'entry':     entry['entry'],
        'levels':    levels
    }

def scan_institutional_style(exchange, valid_pairs: list, style: str, style_cfg: dict) -> int:
    pairs_subset = valid_pairs[:style_cfg['pairs']]
    log.info(f"  {style_cfg['emoji']} [{style_cfg['label']}] Scanning {len(pairs_subset)} pairs on {style_cfg['htf']}...")
    fired = 0
    start = time.time()
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {pool.submit(scan_inst_single, exchange, p, style_cfg['htf'], style_cfg['candles']): p for p in pairs_subset}
        for fut in as_completed(futures):
            try:
                res = fut.result()
                if not res:
                    continue
                if already_signalled(res['symbol'], style, res['sig_type']):
                    continue
                if send_institutional_signal(res['symbol'], res['sig_type'], res['entry'],
                                              res['levels'], style, style_cfg, style_cfg['htf']):
                    mark_signalled(res['symbol'], style, res['sig_type'])
                    log.info(f"    ✅ {style_cfg['emoji']} {res['symbol']} {res['sig_type']} @ {res['entry']} | RR:{res['levels']['rr']}")
                    fired += 1
            except Exception as e:
                pass
    elapsed = round(time.time() - start, 1)
    log.info(f"  {style_cfg['emoji']} [{style_cfg['label']}] Done — {fired} signals in {elapsed}s")
    return fired

# ═══════════════════════════════════════════════════
# FULL SCAN CYCLE
# ═══════════════════════════════════════════════════
def run_full_scan(exchange, valid_pairs: list, exchange_name: str):
    scan_time = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    total_start = time.time()
    log.info("")
    log.info("═" * 62)
    log.info(f"🚀 SignalEdge Dual-Engine Scan — {scan_time}")
    log.info(f"   Exchange: {exchange_name} · Pairs: {len(valid_pairs)}")
    log.info("═" * 62)

    # 1. AI Signals
    ai_fired = 0
    try:
        ai_fired = run_ai_scan(exchange, valid_pairs)
    except Exception as e:
        log.error(f"AI scan error: {e}")

    log.info("")

    # 2. Institutional (all 4 styles)
    inst_total = 0
    for style, cfg in STYLES.items():
        try:
            inst_total += scan_institutional_style(exchange, valid_pairs, style, cfg)
        except Exception as e:
            log.error(f"Inst {style} error: {e}")

    elapsed = round(time.time() - total_start, 1)
    log.info("")
    log.info(f"✅ Full scan complete — AI:{ai_fired} · Inst:{inst_total} · Total time: {elapsed}s")
    log.info(f"⏰ Next scan in {SCAN_INTERVAL // 60} min")
    log.info("")

# ═══════════════════════════════════════════════════
# EXCHANGE AUTO-FALLBACK
# ═══════════════════════════════════════════════════
EXCHANGE_PRIORITY = [
    ("binance",    "Binance Global"),
    ("binanceus",  "Binance US"),
    ("kucoin",     "KuCoin"),
    ("bybit",      "Bybit"),
    ("okx",        "OKX"),
]

def get_valid_pairs(exchange, exchange_name: str) -> list:
    try:
        markets = exchange.load_markets()
        valid = [p for p in PAIRS if p in markets]
        log.info(f"📋 Valid pairs on {exchange_name}: {len(valid)}/{len(PAIRS)}")
        return valid
    except Exception as e:
        log.error(f"Could not load markets on {exchange_name}: {str(e)[:120]}")
        return []

def init_exchange_with_fallback(ccxt_lib):
    last_error = None
    for exchange_id, display_name in EXCHANGE_PRIORITY:
        try:
            log.info(f"🔌 Trying {display_name} ({exchange_id})...")
            exchange_class = getattr(ccxt_lib, exchange_id)
            exchange = exchange_class({'enableRateLimit': True, 'timeout': 15000})
            valid = get_valid_pairs(exchange, display_name)
            if len(valid) >= 10:
                log.info(f"✅ Connected to {display_name} with {len(valid)} valid pairs")
                return exchange, display_name, valid
            log.warning(f"⚠️  {display_name} returned only {len(valid)} pairs, trying next...")
        except Exception as e:
            msg = str(e)[:200]
            last_error = msg
            if "451" in msg or "restricted" in msg.lower() or "eligibility" in msg.lower():
                log.warning(f"🚫 {display_name} is geo-blocked. Trying next...")
            else:
                log.warning(f"⚠️  {display_name} failed: {msg}")
    raise RuntimeError(f"All exchanges failed. Last error: {last_error}")

# ═══════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════
def main():
    import ccxt as ccxt_lib

    log.info("╔══════════════════════════════════════════════════════════╗")
    log.info("║         SignalEdge Triple-Engine Bot v5.0                ║")
    log.info("║  🤖 AI  +  🏦 Institutional  +  📊 Market Scanner      ║")
    log.info("║   Parallel scanning · Auto-exchange fallback             ║")
    log.info("╚══════════════════════════════════════════════════════════╝")
    log.info(f"Institutional webhook: {WEBHOOK_URL}")
    log.info(f"AI webhook:           {AI_WEBHOOK_URL}")
    log.info(f"Market scan webhook:  {SCAN_WEBHOOK_URL}")
    log.info(f"Scan interval:        {SCAN_INTERVAL // 60} min")
    log.info(f"Parallel workers:     {MAX_WORKERS}")
    log.info("")

    exchange, exchange_name, valid_pairs = init_exchange_with_fallback(ccxt_lib)
    log.info("")

    run_full_scan(exchange, valid_pairs, exchange_name)

    while True:
        time.sleep(SCAN_INTERVAL)
        try:
            run_full_scan(exchange, valid_pairs, exchange_name)
        except KeyboardInterrupt:
            log.info("🛑 Bot stopped")
            break
        except Exception as e:
            log.error(f"Unexpected error in main loop: {e}")
            log.error(traceback.format_exc())
            log.info("Restarting in 60 seconds...")
            time.sleep(60)

if __name__ == "__main__":
    main()
