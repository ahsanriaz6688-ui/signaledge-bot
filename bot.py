"""
SignalEdge Bot v5.2 — Triple Engine (PATCHED)
=============================================
🏦 Institutional Engine: Multi-Timeframe Order Block Scanner
🤖 AI Engine:            RSI · MACD · Volume · Breakouts · S/R
📊 Market Scanner:       BUY/SELL/HOLD tags for all pairs

v5.1 PATCHES (April 28):
  1. RSI extreme veto — RSI<=28 zeros bear_score, RSI>=72 zeros bull_score.
  2. TP multipliers bumped from 1.5×/3.0× ATR to 2.5×/4.0× ATR.
  3. Removed tags[:3] truncation — full confluence list now shown.

v5.2 PATCHES (April 28, same day):
  C. Stronger trend filter — require >=0.5% distance from EMA50
     AND RSI in healthy zone (35-65) for trend signals to fire.
  D. AI_MIN_CONFIDENCE bumped 65 → 70 (tighter, fewer false positives).

Author:  SignalEdge
Version: 5.2.0
"""

import os
import time
import logging
import requests
import traceback
import threading
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
FUND_WEBHOOK_URL = os.environ.get("FUND_WEBHOOK_URL", "https://signaledge-server.onrender.com/webhook-fundamentals")
WEBHOOK_SECRET = os.environ.get("WEBHOOK_SECRET", "signaledge2025")
SCAN_INTERVAL  = int(os.environ.get("SCAN_INTERVAL_MINUTES", "1")) * 60
MAX_WORKERS    = int(os.environ.get("MAX_WORKERS", "10"))

CRYPTOPANIC_TOKEN = os.environ.get("CRYPTOPANIC_TOKEN", "")
COINGECKO_API_KEY = os.environ.get("COINGECKO_API_KEY", "")
FUND_INTERVAL_MIN = int(os.environ.get("FUND_INTERVAL_MINUTES", "60"))
FUND_COIN_LIMIT   = int(os.environ.get("FUND_COIN_LIMIT", "250"))

STYLES = {
    "scalp": {
        "label":"Scalping", "htf":"15m", "chart_tf":"5m",
        "lookback":6, "atr_len":14, "atr_mult":1.2, "use_atr":False,
        "sl_buffer_pct":0.0003, "max_bars_wait":25,
        "candles_htf":60, "candles_chart":80,
        "emoji":"⚡", "hold":"Minutes to 2 hours", "pairs":50
    },
    "day": {
        "label":"Day Trade", "htf":"1h", "chart_tf":"15m",
        "lookback":6, "atr_len":14, "atr_mult":1.5, "use_atr":True,
        "sl_buffer_pct":0.0005, "max_bars_wait":25,
        "candles_htf":60, "candles_chart":80,
        "emoji":"📊", "hold":"2–24 hours", "pairs":100
    },
    "swing": {
        "label":"Swing Trade", "htf":"4h", "chart_tf":"1h",
        "lookback":8, "atr_len":14, "atr_mult":1.5, "use_atr":True,
        "sl_buffer_pct":0.001, "max_bars_wait":30,
        "candles_htf":80, "candles_chart":80,
        "emoji":"🎯", "hold":"1–7 days", "pairs":200
    },
    "position": {
        "label":"Position Trade", "htf":"1d", "chart_tf":"4h",
        "lookback":10, "atr_len":14, "atr_mult":2.0, "use_atr":True,
        "sl_buffer_pct":0.002, "max_bars_wait":40,
        "candles_htf":100, "candles_chart":80,
        "emoji":"🏦", "hold":"1–4 weeks", "pairs":100
    },
}

OB_FIB1             = 1.618
OB_FIB2             = 2.0
OB_RETEST_BUFFER    = 0.0
OB_ALLOW_SAMEBAR    = False
OB_MIN_RR           = 1.0

AI_MIN_CONFIDENCE = 65  # v5.3: rolled back from 70 → 65. RSI veto + trend filter still active.
AI_COOLDOWN_MIN   = 240
RSI_OVERSOLD      = 30
RSI_OVERBOUGHT    = 70
VOL_SPIKE_RATIO   = 2.0
MIN_RR            = OB_MIN_RR
MIN_BODY_RATIO    = 0.30
COOLDOWN_HRS      = 2
OB_LOOKBACK       = 6
SL_BUFFER_PCT     = 0.002
FIB1              = OB_FIB1
FIB2              = OB_FIB2

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
# TA INDICATORS
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
    if len(closes) < 35:
        return 0, 0, 0
    ema12 = calc_ema(closes, 12)
    ema26 = calc_ema(closes, 26)
    macd_line = ema12 - ema26
    macd_values = []
    for i in range(26, len(closes)):
        e12 = calc_ema(closes[:i+1], 12)
        e26 = calc_ema(closes[:i+1], 26)
        macd_values.append(e12 - e26)
    signal_line = calc_ema(macd_values, 9) if len(macd_values) >= 9 else 0
    histogram = macd_line - signal_line
    return round(macd_line, 6), round(signal_line, 6), round(histogram, 6)

def calc_volume_spike(volumes: list, lookback: int = 20) -> float:
    if len(volumes) < lookback + 1:
        return 1.0
    current = volumes[-1]
    avg = sum(volumes[-lookback-1:-1]) / lookback
    if avg <= 0.0001:
        return 1.0
    ratio = current / avg
    ratio = min(ratio, 50.0)
    return round(ratio, 2)

def detect_breakout(candles: list, lookback: int = 20) -> str:
    if len(candles) < lookback + 1:
        return ''
    recent_highs = [c[2] for c in candles[-lookback-1:-1]]
    recent_lows  = [c[3] for c in candles[-lookback-1:-1]]
    last_close = candles[-1][4]
    max_high = max(recent_highs)
    min_low  = min(recent_lows)
    if last_close > max_high * 1.002:
        return 'UP'
    if last_close < min_low * 0.998:
        return 'DOWN'
    return ''

def detect_sr_bounce(candles: list, lookback: int = 20) -> str:
    if len(candles) < lookback + 2:
        return ''
    recent_highs = [c[2] for c in candles[-lookback-2:-2]]
    recent_lows  = [c[3] for c in candles[-lookback-2:-2]]
    last_low    = candles[-1][3]
    last_close  = candles[-1][4]
    last_open   = candles[-1][1]
    min_low = min(recent_lows)
    max_high = max(recent_highs)
    if abs(last_low - min_low) / min_low < 0.005 and last_close > last_open:
        return 'SUPPORT'
    last_high = candles[-1][2]
    if abs(last_high - max_high) / max_high < 0.005 and last_close < last_open:
        return 'RESISTANCE'
    return ''

# ═══════════════════════════════════════════════════
# MARKET TAG
# ═══════════════════════════════════════════════════
def compute_market_tag(candles: list) -> dict:
    if len(candles) < 30:
        return {'signal':'hold', 'rsi':50, 'vol_surge':False, 'strength':0}

    closes  = [c[4] for c in candles]
    volumes = [c[5] for c in candles]

    rsi = calc_rsi(closes, 14)
    macd, sig, hist = calc_macd(closes)
    vol_ratio = calc_volume_spike(volumes)

    bull, bear = 0, 0

    if   rsi <= 30:  bull += 70
    elif rsi <= 40:  bull += 45
    elif rsi <= 45:  bull += 20
    elif rsi >= 75:  bear += 70
    elif rsi >= 65:  bear += 45
    elif rsi >= 55:  bear += 20

    if hist > 0 and macd > sig:
        bull += 15
    elif hist < 0 and macd < sig:
        bear += 15

    vol_surge = vol_ratio >= 1.5
    if vol_surge and len(candles) >= 2:
        last = candles[-1]
        if last[4] > last[1]: bull += 10
        else:                 bear += 10

    if len(closes) >= 5 and closes[-5]:
        recent_change = (closes[-1] - closes[-5]) / closes[-5] * 100
        if   recent_change > 2:   bull += 10
        elif recent_change < -2:  bear += 10

    if bull >= 60 and bull > bear + 20:
        signal = 'buy'
        strength = min(bull, 95)
    elif bear >= 60 and bear > bull + 20:
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
# AI SIGNAL ANALYSIS — PATCHED v5.1
# ═══════════════════════════════════════════════════
def analyze_ai(symbol: str, candles: list, htf_candles: list = None) -> dict | None:
    """
    PATCH v5.1 (April 28):
      - RSI EXTREME VETO: if RSI<=28, zero bear_score (block SELL).
        If RSI>=72, zero bull_score (block BUY). Stops the bot from selling
        the bottom and buying the top — the original failure mode.
      - TP1 bumped from 1.5×ATR to 2.5×ATR — R:R 1:1 → 1:1.67.
      - Removed tags[:3] truncation — full confluence now visible.
    """
    if len(candles) < 50:
        return None
    closes  = [c[4] for c in candles]
    volumes = [c[5] for c in candles]

    # Trend filter
    ema50 = calc_ema(closes, 50)
    last_close = closes[-1]
    trend_up   = last_close > ema50
    trend_down = last_close < ema50

    # Volatility regime filter
    atr = _calc_atr(candles, 14)
    atr_pct = (atr / last_close) * 100 if last_close > 0 else 0
    ATR_PCT_MIN = 0.30
    ATR_PCT_MAX = 4.00
    if atr_pct < ATR_PCT_MIN or atr_pct > ATR_PCT_MAX:
        return None

    # 4h HTF confirmation
    htf_rsi = None
    if htf_candles and len(htf_candles) >= 20:
        htf_closes = [c[4] for c in htf_candles]
        htf_rsi = calc_rsi(htf_closes, 14)

    # Indicators
    rsi = calc_rsi(closes, 14)
    macd, sig, hist = calc_macd(closes)
    vol_ratio = calc_volume_spike(volumes)
    breakout  = detect_breakout(candles)
    sr        = detect_sr_bounce(candles)

    # Scoring
    bull_score = 0
    bear_score = 0
    tags = []

    if rsi <= RSI_OVERSOLD:
        bull_score += 30; tags.append('RSI')
    elif rsi >= RSI_OVERBOUGHT:
        bear_score += 30; tags.append('RSI OB')

    if hist > 0 and macd > sig:
        bull_score += 25; tags.append('MACD')
    elif hist < 0 and macd < sig:
        bear_score += 25; tags.append('MACD')

    if vol_ratio >= VOL_SPIKE_RATIO and len(candles) >= 3:
        last3 = candles[-3:]
        green = sum(1 for c in last3 if c[4] > c[1])
        red   = sum(1 for c in last3 if c[4] < c[1])
        if green >= 2:
            bull_score += 20; tags.append('VOL')
        elif red >= 2:
            bear_score += 20; tags.append('VOL')

    if breakout == 'UP':
        bull_score += 25; tags.append('BREAKOUT')
    elif breakout == 'DOWN':
        bear_score += 25; tags.append('BREAKDOWN')

    if sr == 'SUPPORT':
        bull_score += 20; tags.append('SUPPORT')
    elif sr == 'RESISTANCE':
        bear_score += 20; tags.append('RESISTANCE')

    # ═══════════════════════════════════════════
    # PATCH v5.1 — RSI EXTREME VETO
    # When RSI is at a true extreme, mean-reversion dominates.
    # Don't sell deeply oversold (bounce too likely).
    # Don't buy deeply overbought (correction too likely).
    # ═══════════════════════════════════════════
    RSI_EXTREME_LOW  = 28
    RSI_EXTREME_HIGH = 72
    if rsi <= RSI_EXTREME_LOW:
        bear_score = 0  # block SELL on deeply oversold
    elif rsi >= RSI_EXTREME_HIGH:
        bull_score = 0  # block BUY on deeply overbought

    # HTF gate
    htf_blocks_buy  = htf_rsi is not None and htf_rsi >= 75
    htf_blocks_sell = htf_rsi is not None and htf_rsi <= 25

    # ═══════════════════════════════════════════
    # PATCH v5.2 — STRONGER TREND FILTER (fix C)
    # Old filter: just price > EMA50 for bull, price < EMA50 for bear.
    # Problem: in sideways markets price hugs EMA50, flipping the filter
    # on every candle and creating noise.
    # New filter: require price to be at least 0.5% away from EMA50
    # AND require RSI in a "healthy trend" zone (35-65). This ensures
    # the trend has actual momentum, not just a pixel of separation.
    # ═══════════════════════════════════════════
    ema_distance_pct = abs(last_close - ema50) / ema50 * 100 if ema50 > 0 else 0
    EMA_MIN_DISTANCE = 0.5  # require >= 0.5% separation from EMA50
    RSI_TREND_LOW    = 35   # below this: oversold zone, no trend SELL
    RSI_TREND_HIGH   = 65   # above this: overbought zone, no trend BUY

    strong_trend_up   = trend_up   and ema_distance_pct >= EMA_MIN_DISTANCE and rsi <= RSI_TREND_HIGH
    strong_trend_down = trend_down and ema_distance_pct >= EMA_MIN_DISTANCE and rsi >= RSI_TREND_LOW

    # Decide direction
    if (bull_score >= AI_MIN_CONFIDENCE
            and bull_score > bear_score
            and strong_trend_up
            and not htf_blocks_buy):
        direction = 'BUY'; confidence = min(bull_score, 95)
    elif (bear_score >= AI_MIN_CONFIDENCE
            and bear_score > bull_score
            and strong_trend_down
            and not htf_blocks_sell):
        direction = 'SELL'; confidence = min(bear_score, 95)
    else:
        return None

    if len(tags) < 2:
        return None

    last_c = candles[-1]
    price = last_c[4]

    # Build reasoning summary
    reason_parts = []
    rsi_display = max(rsi, 1.0)
    if rsi <= RSI_OVERSOLD: reason_parts.append(f"RSI deeply oversold at {rsi_display:.1f}")
    elif rsi >= RSI_OVERBOUGHT: reason_parts.append(f"RSI overbought at {rsi_display:.1f}")
    if 'MACD' in tags: reason_parts.append("MACD " + ("bullish crossover" if direction=='BUY' else "bearish crossover"))
    if 'VOL' in tags: reason_parts.append(f"Volume {vol_ratio}x average")
    if breakout == 'UP': reason_parts.append("Resistance breakout")
    if breakout == 'DOWN': reason_parts.append("Support breakdown")
    if sr == 'SUPPORT': reason_parts.append("Support bounce")
    if sr == 'RESISTANCE': reason_parts.append("Resistance rejection")

    reason = '. '.join(reason_parts[:3]) + '.'

    # ATR-based SL/TP
    atr = 0.0
    if len(candles) >= 15:
        trs = []
        for i in range(-14, 0):
            hi, lo, prev_close = candles[i][2], candles[i][3], candles[i-1][4]
            tr = max(hi - lo, abs(hi - prev_close), abs(lo - prev_close))
            trs.append(tr)
        atr = sum(trs) / len(trs) if trs else 0.0

    if atr <= 0:
        atr = price * 0.015

    # ═══════════════════════════════════════════
    # PATCH v5.1 — TP multipliers bumped for R:R 1:1.67
    # SL = 1.5× ATR, TP1 = 2.5× ATR, TP2 = 4× ATR
    # ═══════════════════════════════════════════
    if direction == 'BUY':
        sl  = round(price - 1.5 * atr, 8)
        tp1 = round(price + 2.5 * atr, 8)
        tp2 = round(price + 4.0 * atr, 8)
    else:
        sl  = round(price + 1.5 * atr, 8)
        tp1 = round(price - 2.5 * atr, 8)
        tp2 = round(price - 4.0 * atr, 8)

    return {
        'symbol':     symbol.replace('/USDT', ''),
        'type':       direction.lower(),
        'price':      price,
        'entry':      price,
        'sl':         sl,
        'tp1':        tp1,
        'tp2':        tp2,
        'confidence': confidence,
        'tags':       tags,  # PATCH v5.1: was tags[:3]
        'reason':     reason,
        'rsi':        rsi,
        'timeframe':  '1h'
    }

# ═══════════════════════════════════════════════════
# INSTITUTIONAL ENGINE (UNCHANGED)
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
# LIQUIDITY SWEEP ENGINE (UNCHANGED)
# ═══════════════════════════════════════════════════

LS_SWING_LOOKBACK  = 20
LS_MIN_RETEST_BARS = 2
LS_MAX_RETEST_BARS = 15
LS_VOL_MULT        = 1.1
LS_SL_BUFFER_PCT   = 0.002
LS_MIN_RR          = 0.8
LS_TP1_MULT        = 1.0
LS_TP2_MULT        = 1.2
LS_TP3_MULT        = 1.4

def _find_swing_high(candles: list, lookback: int = 20) -> tuple:
    if len(candles) < lookback + 2:
        return None, None
    search = candles[-lookback-5:-3] if len(candles) > lookback + 5 else candles[:-3]
    if not search:
        return None, None
    high_idx = max(range(len(search)), key=lambda i: search[i][2])
    high_price = search[high_idx][2]
    actual_idx = len(candles) - len(search[-len(search):]) - 3 + high_idx
    return actual_idx, high_price

def _find_swing_low(candles: list, lookback: int = 20) -> tuple:
    if len(candles) < lookback + 2:
        return None, None
    search = candles[-lookback-5:-3] if len(candles) > lookback + 5 else candles[:-3]
    if not search:
        return None, None
    low_idx = min(range(len(search)), key=lambda i: search[i][3])
    low_price = search[low_idx][3]
    actual_idx = len(candles) - len(search[-len(search):]) - 3 + low_idx
    return actual_idx, low_price

def _detect_liquidity_sweep(candles: list) -> dict | None:
    if len(candles) < LS_SWING_LOOKBACK + 5:
        return None
    sh_idx, sh_price = _find_swing_high(candles, LS_SWING_LOOKBACK)
    sl_idx, sl_price = _find_swing_low(candles, LS_SWING_LOOKBACK)
    for i in range(max(0, len(candles) - 10), len(candles) - 1):
        c = candles[i]
        high, low = c[2], c[3]
        if sh_price and high > sh_price:
            if c[4] < sh_price:
                return {'type': 'bear_sweep', 'sweep_level': sh_price, 'sweep_idx': i, 'swept_high': high}
        if sl_price and low < sl_price:
            if c[4] > sl_price:
                return {'type': 'bull_sweep', 'sweep_level': sl_price, 'sweep_idx': i, 'swept_low': low}
    return None

def _check_sweep_retest(candles: list, sweep: dict) -> dict | None:
    if not sweep or len(candles) < 2:
        return None
    sweep_idx = sweep['sweep_idx']
    current_idx = len(candles) - 1
    bars_since = current_idx - sweep_idx
    if bars_since < LS_MIN_RETEST_BARS or bars_since > LS_MAX_RETEST_BARS:
        return None
    last = candles[-1]
    o, h, l, c, v = last[1], last[2], last[3], last[4], last[5]
    level = sweep['sweep_level']
    recent_vols = [candles[i][5] for i in range(max(0, len(candles)-20), len(candles)-1)]
    avg_vol = sum(recent_vols) / len(recent_vols) if recent_vols else 0
    vol_ok = avg_vol > 0 and v >= avg_vol * LS_VOL_MULT
    if not vol_ok:
        return None
    if sweep['type'] == 'bull_sweep':
        if l <= level * 1.005 and c > level:
            swept_low = sweep.get('swept_low', level)
            move = c - swept_low
            if move <= 0:
                return None
            return {'type': 'BUY', 'entry': c, 'level': level, 'swept_extreme': swept_low, 'move': move}
    elif sweep['type'] == 'bear_sweep':
        if h >= level * 0.995 and c < level:
            swept_high = sweep.get('swept_high', level)
            move = swept_high - c
            if move <= 0:
                return None
            return {'type': 'SELL', 'entry': c, 'level': level, 'swept_extreme': swept_high, 'move': move}
    return None

def _calc_sweep_levels(signal: dict) -> dict:
    entry = signal['entry']
    level = signal['level']
    swept = signal['swept_extreme']
    move  = signal['move']
    sig_type = signal['type']
    if sig_type == 'BUY':
        sl = swept * (1 - LS_SL_BUFFER_PCT)
        tp1 = entry + move * LS_TP1_MULT
        tp2 = entry + move * LS_TP2_MULT
        tp3 = entry + move * LS_TP3_MULT
        risk = entry - sl
    else:
        sl = swept * (1 + LS_SL_BUFFER_PCT)
        tp1 = entry - move * LS_TP1_MULT
        tp2 = entry - move * LS_TP2_MULT
        tp3 = entry - move * LS_TP3_MULT
        risk = sl - entry
    if risk <= 0:
        return None
    rr = round(abs(tp1 - entry) / risk, 2)
    return {
        'sl': round(sl, 8), 'tp1': round(tp1, 8), 'tp2': round(tp2, 8), 'tp3': round(tp3, 8),
        'risk_pct': round(risk / entry * 100, 2), 'rr': rr
    }

def scan_liquidity_sweep(exchange, symbol: str) -> dict | None:
    try:
        candles = fetch_ohlcv(exchange, symbol, '15m', 80)
        if not candles or len(candles) < 30:
            return None
        closed = candles[:-1]
        if len(closed) < 30:
            return None
        sweep = _detect_liquidity_sweep(closed)
        if not sweep:
            return None
        signal = _check_sweep_retest(closed, sweep)
        if not signal:
            return None
        levels = _calc_sweep_levels(signal)
        if not levels or levels['rr'] < LS_MIN_RR:
            return None
        return {'symbol': symbol, 'sig_type': signal['type'], 'entry': round(signal['entry'], 8), 'levels': levels, 'sweep_type': sweep['type']}
    except Exception as e:
        log.debug(f"[SWEEP] {symbol} error: {e}")
        return None

_sweep_cooldown = {}
_sweep_cooldown_lock = threading.Lock()

def _sweep_already_signalled(symbol: str, direction: str) -> bool:
    with _sweep_cooldown_lock:
        key = f"{symbol}_{direction}"
        if key in _sweep_cooldown:
            elapsed = (datetime.now(timezone.utc) - _sweep_cooldown[key]).total_seconds()
            if elapsed < 2 * 3600:
                return True
        return False

def _sweep_mark_signalled(symbol: str, direction: str):
    with _sweep_cooldown_lock:
        _sweep_cooldown[f"{symbol}_{direction}"] = datetime.now(timezone.utc)

def send_sweep_signal(symbol: str, sig_type: str, entry: float, levels: dict) -> bool:
    payload = {
        'secret': WEBHOOK_SECRET, 'symbol': symbol.replace('/USDT', '').replace('/USD', ''),
        'type': sig_type, 'price': entry, 'sl': levels['sl'], 'tp1': levels['tp1'],
        'tp2': levels['tp2'], 'tp3': levels['tp3'], 'rr': levels['rr'], 'risk_pct': levels['risk_pct'],
        'timeframe': '15m', 'style': 'sweep', 'style_label': 'Liquidity Sweep',
        'hold': 'Minutes to hours', 'strategy': 'Liquidity Sweep + Retest',
        'timestamp': datetime.now(timezone.utc).isoformat()
    }
    try:
        r = requests.post(WEBHOOK_URL, json=payload, timeout=10)
        return r.status_code == 200
    except Exception as e:
        log.error(f"[SWEEP] Webhook error: {e}")
        return False

def run_liquidity_sweep_scan(exchange, valid_pairs: list) -> int:
    log.info(f"  🌊 [Liquidity Sweep] Scanning {len(valid_pairs)} pairs on 15m...")
    fired = 0
    start = time.time()
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {pool.submit(scan_liquidity_sweep, exchange, p): p for p in valid_pairs}
        for fut in as_completed(futures):
            try:
                res = fut.result()
                if not res:
                    continue
                if _sweep_already_signalled(res['symbol'], res['sig_type']):
                    continue
                if send_sweep_signal(res['symbol'], res['sig_type'], res['entry'], res['levels']):
                    _sweep_mark_signalled(res['symbol'], res['sig_type'])
                    log.info(f"    🌊 {res['symbol']} {res['sig_type']} @ {res['entry']} | RR:{res['levels']['rr']}")
                    fired += 1
            except Exception as e:
                pass
    elapsed = round(time.time() - start, 1)
    log.info(f"  🌊 [Liquidity Sweep] Done in {elapsed}s — {fired} signals fired")
    return fired

# ═══════════════════════════════════════════════════
# OB STRATEGY ENGINE (UNCHANGED)
# ═══════════════════════════════════════════════════

_ob_state = {}
_ob_state_lock = threading.Lock()

def _get_ob_state(symbol: str, style: str) -> dict:
    key = f"{symbol}|{style}"
    with _ob_state_lock:
        if key not in _ob_state:
            _ob_state[key] = {
                'ob_high': None, 'ob_low': None, 'ob_bull': False, 'ob_id': 0,
                'flip_armed': False, 'flip_dir': 0, 'flip_level': None,
                'flip_id': 0, 'flip_bar_time': None,
                'ob_high_at_flip': None, 'ob_low_at_flip': None,
                'bars_since_flip': 0, 'last_htf_time': None
            }
        return _ob_state[key]

def _calc_atr(candles: list, length: int) -> float:
    if len(candles) < length + 1:
        return 0.0
    trs = []
    for i in range(1, len(candles)):
        h = candles[i][2]; l = candles[i][3]; pc = candles[i-1][4]
        tr = max(h - l, abs(h - pc), abs(l - pc))
        trs.append(tr)
    recent = trs[-length:]
    return sum(recent) / len(recent) if recent else 0.0

def _find_last_bear_candle(candles: list, lookback: int, use_body: bool = False) -> tuple:
    for i in range(1, lookback + 1):
        idx = -1 - i
        if abs(idx) > len(candles):
            break
        c = candles[idx]
        o, h, l, cl = c[1], c[2], c[3], c[4]
        if cl < o:
            if use_body:
                return max(o, cl), min(o, cl)
            else:
                return h, l
    return None, None

def _find_last_bull_candle(candles: list, lookback: int, use_body: bool = False) -> tuple:
    for i in range(1, lookback + 1):
        idx = -1 - i
        if abs(idx) > len(candles):
            break
        c = candles[idx]
        o, h, l, cl = c[1], c[2], c[3], c[4]
        if cl > o:
            if use_body:
                return max(o, cl), min(o, cl)
            else:
                return h, l
    return None, None

def _detect_new_ob(htf_candles: list, style_cfg: dict) -> dict | None:
    if len(htf_candles) < 3:
        return None
    last = htf_candles[-1]
    prev = htf_candles[-2]
    lookback = style_cfg.get('lookback', 6)
    use_atr  = style_cfg.get('use_atr', False)
    atr_mult = style_cfg.get('atr_mult', 1.5)
    atr_len  = style_cfg.get('atr_len', 14)
    o, h, l, c = last[1], last[2], last[3], last[4]
    prev_h, prev_l = prev[2], prev[3]
    if use_atr:
        atr = _calc_atr(htf_candles, atr_len)
        if atr > 0 and (h - l) < atr * atr_mult:
            return None
    if c > prev_h and o < c:
        bull_h, bull_l = _find_last_bear_candle(htf_candles, lookback, use_body=False)
        if bull_h is not None and bull_l is not None:
            return {'type': 'bull', 'high': bull_h, 'low': bull_l}
    if c < prev_l and o > c:
        bear_h, bear_l = _find_last_bull_candle(htf_candles, lookback, use_body=False)
        if bear_h is not None and bear_l is not None:
            return {'type': 'bear', 'high': bear_h, 'low': bear_l}
    return None

def _check_flip(htf_close: float, state: dict) -> bool:
    if state['ob_high'] is None or state['ob_low'] is None:
        return False
    if state['flip_armed']:
        return False
    if state['ob_bull'] and htf_close < state['ob_low']:
        state['flip_armed'] = True; state['flip_dir'] = 1; state['flip_level'] = state['ob_low']
        state['flip_id'] = state['ob_id']; state['ob_high_at_flip'] = state['ob_high']
        state['ob_low_at_flip'] = state['ob_low']; state['bars_since_flip'] = 0
        return True
    if (not state['ob_bull']) and htf_close > state['ob_high']:
        state['flip_armed'] = True; state['flip_dir'] = -1; state['flip_level'] = state['ob_high']
        state['flip_id'] = state['ob_id']; state['ob_high_at_flip'] = state['ob_high']
        state['ob_low_at_flip'] = state['ob_low']; state['bars_since_flip'] = 0
        return True
    return False

def _check_retest(chart_candles: list, state: dict, style_cfg: dict) -> dict | None:
    if not state['flip_armed'] or state['flip_level'] is None:
        return None
    if len(chart_candles) < 2:
        return None
    last = chart_candles[-1]
    h, l, c = last[2], last[3], last[4]
    lvl = state['flip_level']
    buf = OB_RETEST_BUFFER
    if state['flip_dir'] == 1:
        if h >= lvl + buf and c < lvl:
            return {'type': 'SELL', 'entry': lvl, 'ob_high': state['ob_high_at_flip'], 'ob_low': state['ob_low_at_flip']}
    if state['flip_dir'] == -1:
        if l <= lvl - buf and c > lvl:
            return {'type': 'BUY', 'entry': lvl, 'ob_high': state['ob_high_at_flip'], 'ob_low': state['ob_low_at_flip']}
    return None

def _calc_ob_levels(entry: float, ob_high_at_flip: float, ob_low_at_flip: float,
                    sig_type: str, sl_buffer_pct: float) -> dict:
    rng = abs(ob_high_at_flip - ob_low_at_flip)
    if sig_type == 'SELL':
        sl = ob_high_at_flip * (1 + sl_buffer_pct)
        tp1 = entry - rng; tp2 = entry - rng * OB_FIB1; tp3 = entry - rng * OB_FIB2
        risk = sl - entry
    else:
        sl = ob_low_at_flip * (1 - sl_buffer_pct)
        tp1 = entry + rng; tp2 = entry + rng * OB_FIB1; tp3 = entry + rng * OB_FIB2
        risk = entry - sl
    if risk <= 0:
        return None
    rr = round(abs(tp1 - entry) / risk, 2)
    return {
        'sl': round(sl, 8), 'tp1': round(tp1, 8), 'tp2': round(tp2, 8), 'tp3': round(tp3, 8),
        'risk_pct': round(risk / entry * 100, 2), 'rr': rr
    }

def scan_ob_strategy(exchange, symbol: str, style: str, style_cfg: dict) -> dict | None:
    try:
        state = _get_ob_state(symbol, style)
        is_fresh_state = state['last_htf_time'] is None
        htf_candles = fetch_ohlcv(exchange, symbol, style_cfg['htf'], style_cfg['candles_htf'])
        if not htf_candles or len(htf_candles) < style_cfg['lookback'] + 5:
            return None
        htf_closed = htf_candles[:-1]
        if len(htf_closed) < style_cfg['lookback'] + 5:
            return None
        if is_fresh_state:
            replay_bars = min(40, len(htf_closed) - style_cfg['lookback'] - 1)
            replay_start = len(htf_closed) - replay_bars
            for idx in range(replay_start, len(htf_closed)):
                sub = htf_closed[:idx+1]
                if len(sub) < style_cfg['lookback'] + 2:
                    continue
                bar_close = sub[-1][4]
                flip_just_fired = _check_flip(bar_close, state)
                new_ob = _detect_new_ob(sub, style_cfg)
                if new_ob:
                    new_bull = (new_ob['type'] == 'bull')
                    should_replace = state['ob_high'] is None or state['ob_bull'] != new_bull
                    if should_replace:
                        state['ob_high'] = new_ob['high']; state['ob_low'] = new_ob['low']
                        state['ob_bull'] = new_bull; state['ob_id'] += 1
                        if not flip_just_fired:
                            state['flip_armed'] = False; state['flip_dir'] = 0; state['flip_level'] = None
                if state['flip_armed']:
                    state['bars_since_flip'] += 1
                    if state['bars_since_flip'] > style_cfg['max_bars_wait']:
                        state['flip_armed'] = False; state['flip_dir'] = 0
                        state['flip_level'] = None; state['bars_since_flip'] = 0
            state['last_htf_time'] = htf_closed[-1][0]
        last_htf = htf_closed[-1]
        htf_time = last_htf[0]; htf_close = last_htf[4]
        is_new_htf_bar = (state['last_htf_time'] != htf_time) and not is_fresh_state
        if is_new_htf_bar:
            state['last_htf_time'] = htf_time
            flip_just_fired = _check_flip(htf_close, state)
            new_ob = _detect_new_ob(htf_closed, style_cfg)
            if new_ob:
                new_type_is_bull = (new_ob['type'] == 'bull')
                should_replace = (state['ob_high'] is None) or (state['ob_bull'] != new_type_is_bull)
                if should_replace:
                    state['ob_high'] = new_ob['high']; state['ob_low'] = new_ob['low']
                    state['ob_bull'] = new_type_is_bull; state['ob_id'] += 1
                    if not flip_just_fired:
                        state['flip_armed'] = False; state['flip_dir'] = 0; state['flip_level'] = None
            if state['flip_armed']:
                state['bars_since_flip'] += 1
                if state['bars_since_flip'] > style_cfg['max_bars_wait']:
                    state['flip_armed'] = False; state['flip_dir'] = 0
                    state['flip_level'] = None; state['flip_id'] = 0; state['bars_since_flip'] = 0
        if not state['flip_armed']:
            return None
        chart_candles = fetch_ohlcv(exchange, symbol, style_cfg['chart_tf'], style_cfg['candles_chart'])
        if not chart_candles or len(chart_candles) < 2:
            return None
        retest = _check_retest(chart_candles, state, style_cfg)
        if not retest:
            return None
        levels = _calc_ob_levels(retest['entry'], retest['ob_high'], retest['ob_low'],
                                  retest['type'], style_cfg['sl_buffer_pct'])
        if not levels or levels['rr'] < OB_MIN_RR:
            return None
        state['flip_armed'] = False; state['flip_dir'] = 0; state['flip_level'] = None
        return {'symbol': symbol, 'sig_type': retest['type'], 'entry': round(retest['entry'], 8), 'levels': levels}
    except Exception as e:
        log.debug(f"[OB] {symbol}/{style} error: {e}")
        return None

def clear_ob_state_for_fired(symbol: str, style: str):
    key = f"{symbol}|{style}"
    with _ob_state_lock:
        _ob_state.pop(key, None)

# ═══════════════════════════════════════════════════
# WEBHOOK SENDERS
# ═══════════════════════════════════════════════════
def send_institutional_signal(symbol: str, sig_type: str, entry: float, levels: dict,
                              style: str, style_cfg: dict, timeframe: str) -> bool:
    payload = {
        'secret': WEBHOOK_SECRET, 'type': sig_type, 'symbol': symbol.replace('/USDT', 'USDT'),
        'price': entry, 'sl': levels['sl'], 'tp1': levels['tp1'], 'tp2': levels['tp2'], 'tp3': levels['tp3'],
        'timeframe': timeframe, 'strategy': 'SignalEdge Institutional', 'style': style,
        'style_label': style_cfg['label'], 'hold': style_cfg['hold'],
        'risk_pct': levels['risk_pct'], 'rr': levels['rr'],
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
        'secret': WEBHOOK_SECRET, 'symbol': signal['symbol'], 'type': signal['type'],
        'price': signal['price'], 'entry': signal.get('entry', signal['price']),
        'sl': signal.get('sl', 0), 'tp1': signal.get('tp1', 0), 'tp2': signal.get('tp2', 0),
        'confidence': signal['confidence'], 'tags': signal['tags'], 'reason': signal['reason'],
        'rsi': signal['rsi'], 'timeframe': signal['timeframe'],
        'timestamp': datetime.now(timezone.utc).isoformat()
    }
    try:
        r = requests.post(AI_WEBHOOK_URL, json=payload, timeout=8)
        if r.status_code == 200:
            return True
        log.warning(f"  ⚠️ AI webhook {r.status_code} for {signal['symbol']}")
    except Exception as e:
        log.error(f"  ❌ AI webhook error for {signal['symbol']}: {e}")
    return False

def fetch_ohlcv(exchange, symbol: str, timeframe: str, limit: int = 150):
    for attempt in range(3):
        try:
            return exchange.fetch_ohlcv(symbol, timeframe, limit=limit)
        except Exception as e:
            err_str = str(e).lower()
            if 'rate' in err_str or 'limit' in err_str or '429' in err_str:
                log.warning(f"[OHLCV] rate-limited on {symbol} {timeframe}, sleeping 2s (attempt {attempt+1})")
                time.sleep(2)
            elif attempt == 2:
                log.debug(f"[OHLCV] {symbol} {timeframe} failed 3x: {e}")
                return None
            else:
                time.sleep(0.5)
    return None

def scan_ai_single(exchange, symbol: str) -> dict:
    result = {'symbol': symbol.replace('/USDT', '').replace('/USD', ''), 'tag': None, 'signal': None}
    candles = fetch_ohlcv(exchange, symbol, '1h', 100)
    if not candles or len(candles) < 30:
        return result
    result['tag'] = compute_market_tag(candles)
    if len(candles) >= 50:
        htf_candles = fetch_ohlcv(exchange, symbol, '4h', 50)
        sig = analyze_ai(symbol, candles, htf_candles)
        if sig and not already_ai_signalled(sig['symbol'], sig['type']):
            result['signal'] = sig
    return result

def send_market_scan(coins_dict: dict) -> bool:
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
    market_tags = {}
    start = time.time()
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {pool.submit(scan_ai_single, exchange, p): p for p in valid_pairs}
        for fut in as_completed(futures):
            try:
                res = fut.result()
                if not res:
                    continue
                if res.get('tag') and res.get('symbol'):
                    market_tags[res['symbol']] = res['tag']
                sig = res.get('signal')
                if sig:
                    if send_ai_signal(sig):
                        mark_ai_signalled(sig['symbol'], sig['type'])
                        log.info(f"  🤖 {sig['symbol']} {sig['type'].upper()} @ {sig['price']} | conf:{sig['confidence']}% | tags:{','.join(sig['tags'])}")
                        fired += 1
            except Exception as e:
                pass
    elapsed = round(time.time() - start, 1)
    if market_tags:
        ok = send_market_scan(market_tags)
        buy_n = sum(1 for t in market_tags.values() if t['signal'] == 'buy')
        sell_n = sum(1 for t in market_tags.values() if t['signal'] == 'sell')
        hold_n = sum(1 for t in market_tags.values() if t['signal'] == 'hold')
        log.info(f"📊 Market scan: {len(market_tags)} coins → Buy:{buy_n} · Sell:{sell_n} · Hold:{hold_n} → upload {'✅' if ok else '❌'}")
    log.info(f"🤖 [AI Signals] Done — {fired} signals in {elapsed}s")
    return fired

def scan_inst_single(exchange, symbol: str, style: str, style_cfg: dict) -> dict | None:
    return scan_ob_strategy(exchange, symbol, style, style_cfg)

def scan_institutional_style(exchange, valid_pairs: list, style: str, style_cfg: dict) -> int:
    pairs_subset = valid_pairs[:style_cfg['pairs']]
    log.info(f"  {style_cfg['emoji']} [{style_cfg['label']}] Scanning {len(pairs_subset)} pairs — HTF:{style_cfg['htf']} · retest:{style_cfg['chart_tf']}...")
    fired = 0
    start = time.time()
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {pool.submit(scan_inst_single, exchange, p, style, style_cfg): p for p in pairs_subset}
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
    armed = sum(1 for k, v in _ob_state.items() if k.endswith(f"|{style}") and v.get('flip_armed'))
    has_ob = sum(1 for k, v in _ob_state.items() if k.endswith(f"|{style}") and v.get('ob_high') is not None)
    elapsed = round(time.time() - start, 1)
    log.info(f"  {style_cfg['emoji']} [{style_cfg['label']}] Done in {elapsed}s — Signals:{fired} · OBs tracked:{has_ob} · Flips armed:{armed}")
    return fired

def run_full_scan(exchange, valid_pairs: list, exchange_name: str):
    scan_time = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    total_start = time.time()
    log.info("")
    log.info("═" * 62)
    log.info(f"🚀 SignalEdge Dual-Engine Scan — {scan_time}")
    log.info(f"   Exchange: {exchange_name} · Pairs: {len(valid_pairs)}")
    log.info("═" * 62)
    ai_fired = 0
    try:
        ai_fired = run_ai_scan(exchange, valid_pairs)
    except Exception as e:
        log.error(f"AI scan error: {e}")
    log.info("")
    sweep_fired = 0
    try:
        sweep_fired = run_liquidity_sweep_scan(exchange, valid_pairs)
    except Exception as e:
        log.error(f"Sweep scan error: {e}")
    log.info("")
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

EXCHANGE_PRIORITY = [
    ("binance", "Binance Global"), ("binanceus", "Binance US"),
    ("kucoin", "KuCoin"), ("bybit", "Bybit"), ("okx", "OKX"),
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
            import urllib3
            import requests.adapters
            session = requests.Session()
            adapter = requests.adapters.HTTPAdapter(
                pool_connections=50, pool_maxsize=50,
                max_retries=urllib3.util.Retry(total=2, backoff_factor=0.3,
                                               status_forcelist=[429, 500, 502, 503, 504])
            )
            session.mount('https://', adapter)
            session.mount('http://', adapter)
            exchange = exchange_class({'enableRateLimit': True, 'timeout': 15000, 'session': session})
            valid = get_valid_pairs(exchange, display_name)
            if len(valid) >= 10:
                log.info(f"✅ Connected to {display_name} with {len(valid)} valid pairs (pool=50)")
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
# FUNDAMENTALS ENGINE
# ═══════════════════════════════════════════════════
def _cg_headers():
    if COINGECKO_API_KEY:
        return {"x-cg-demo-api-key": COINGECKO_API_KEY}
    return {}

def _cg_rate_sleep():
    return 2.2 if COINGECKO_API_KEY else 12.0

def fetch_coingecko_top_coins(limit=500):
    all_coins = []
    pages = (limit + 249) // 250
    try:
        for page in range(1, pages + 1):
            per_page = min(250, limit - len(all_coins))
            if per_page <= 0: break
            url = f"https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&order=market_cap_desc&per_page={per_page}&page={page}&sparkline=false"
            r = requests.get(url, headers=_cg_headers(), timeout=15)
            if r.status_code != 200:
                log.warning(f"[FUND] CoinGecko top coins page {page} returned {r.status_code}")
                break
            data = r.json()
            if not data:
                break
            all_coins.extend([{
                'id': c.get('id'), 'symbol': (c.get('symbol') or '').upper(),
                'name': c.get('name'), 'rank': c.get('market_cap_rank'),
                'mcap': c.get('market_cap'), 'price': c.get('current_price'),
                'ath_change_percentage': c.get('ath_change_percentage', -100)
            } for c in data if c.get('id')])
            time.sleep(_cg_rate_sleep())
        return all_coins
    except Exception as e:
        log.warning(f"[FUND] Top coins fetch failed: {e}")
        return all_coins

def fetch_coingecko_coin_detail(coin_id):
    try:
        url = f"https://api.coingecko.com/api/v3/coins/{coin_id}?localization=false&tickers=false&community_data=true&developer_data=true&sparkline=false"
        r = requests.get(url, headers=_cg_headers(), timeout=15)
        if r.status_code == 429:
            log.warning(f"[FUND] CoinGecko 429 rate-limit on {coin_id}, sleeping 60s")
            time.sleep(60)
            return None
        if r.status_code == 401:
            log.warning(f"[FUND] CoinGecko 401 unauthorized — check COINGECKO_API_KEY")
            return None
        if r.status_code != 200:
            log.warning(f"[FUND] CoinGecko {r.status_code} on {coin_id}")
            return None
        d = r.json()
        result = {
            'dev_score': float(d.get('developer_score') or 0),
            'community_score': float(d.get('community_score') or 0),
            'liquidity_score': float(d.get('liquidity_score') or 0),
            'coingecko_score': float(d.get('coingecko_score') or 0),
            'public_interest_score': float(d.get('public_interest_score') or 0),
            'reddit_subscribers': (d.get('community_data') or {}).get('reddit_subscribers') or 0,
            'github_commits_4w': (d.get('developer_data') or {}).get('commit_count_4_weeks') or 0,
            'github_stars': (d.get('developer_data') or {}).get('stars') or 0,
            'genesis_date': d.get('genesis_date'),
            'categories': d.get('categories') or []
        }
        if coin_id in ('bitcoin', 'ethereum'):
            log.info(f"[FUND] {coin_id} scores: dev={result['dev_score']:.1f} comm={result['community_score']:.1f} liq={result['liquidity_score']:.1f}")
        return result
    except requests.exceptions.Timeout:
        log.warning(f"[FUND] CoinGecko timeout on {coin_id}")
        return None
    except Exception as e:
        log.warning(f"[FUND] CoinGecko error on {coin_id}: {e}")
        return None

def fetch_defillama_tvl():
    try:
        r = requests.get("https://api.llama.fi/protocols", timeout=15)
        if r.status_code != 200:
            return {}
        data = r.json()
        by_symbol = {}
        for p in data:
            sym = (p.get('symbol') or '').upper()
            if not sym or sym == '-':
                continue
            if sym not in by_symbol or (p.get('tvl') or 0) > (by_symbol[sym].get('tvl') or 0):
                by_symbol[sym] = {
                    'tvl': p.get('tvl', 0) or 0, 'change_1d': p.get('change_1d', 0) or 0,
                    'change_7d': p.get('change_7d', 0) or 0, 'category': p.get('category', '')
                }
        log.info(f"[FUND] DefiLlama: {len(by_symbol)} protocols indexed")
        return by_symbol
    except Exception as e:
        log.warning(f"[FUND] DefiLlama fetch failed: {e}")
        return {}

def fetch_cryptopanic_sentiment(symbol):
    if not CRYPTOPANIC_TOKEN:
        return None
    try:
        url = f"https://cryptopanic.com/api/v1/posts/?auth_token={CRYPTOPANIC_TOKEN}&currencies={symbol}&filter=hot&public=true"
        r = requests.get(url, timeout=8)
        if r.status_code != 200:
            return None
        posts = r.json().get('results', [])
        pos = sum((p.get('votes', {}).get('positive', 0) or 0) for p in posts[:20])
        neg = sum((p.get('votes', {}).get('negative', 0) or 0) for p in posts[:20])
        imp = sum((p.get('votes', {}).get('important', 0) or 0) for p in posts[:20])
        return {'positive': pos, 'negative': neg, 'important': imp, 'count': len(posts)}
    except Exception as e:
        return None

def calc_fundamental_score(coin_meta, detail, tvl, news):
    score = 0.0
    breakdown = {}
    if detail:
        dev = detail.get('dev_score', 0) or 0
        if dev == 0:
            commits_4w = detail.get('github_commits_4w', 0) or 0
            stars = detail.get('github_stars', 0) or 0
            dev_from_commits = min(60, commits_4w * 0.6) if commits_4w > 0 else 0
            dev_from_stars = min(40, stars / 25) if stars > 0 else 0
            dev = dev_from_commits + dev_from_stars
    else:
        dev = 0
    score += dev * 0.25
    breakdown['developer'] = round(dev, 1)
    if detail:
        comm = detail.get('community_score', 0) or 0
        if comm == 0:
            reddit = detail.get('reddit_subscribers', 0) or 0
            comm = min(100, reddit / 1000) if reddit > 0 else 0
    else:
        comm = 0
    score += comm * 0.20
    breakdown['community'] = round(comm, 1)
    if detail:
        liq = detail.get('liquidity_score', 0) or 0
        if liq == 0:
            rank = coin_meta.get('rank', 9999) or 9999
            if   rank <= 10:   liq = 90
            elif rank <= 30:   liq = 75
            elif rank <= 100:  liq = 60
            elif rank <= 250:  liq = 40
            elif rank <= 500:  liq = 20
            else:              liq = 5
    else:
        liq = 0
    score += liq * 0.15
    breakdown['liquidity'] = round(liq, 1)
    ath_pct = coin_meta.get('ath_change_percentage', -100) or -100
    if   ath_pct >= -20: ath_points = 10
    elif ath_pct >= -40: ath_points = 8
    elif ath_pct >= -60: ath_points = 5
    elif ath_pct >= -80: ath_points = 2
    else:                ath_points = 0
    score += ath_points
    breakdown['ath_pct'] = round(ath_pct, 1)
    news_contrib = 0
    if news and (news['positive'] + news['negative']) > 0:
        ratio = news['positive'] / (news['positive'] + news['negative'])
        news_contrib = ratio * 15
        breakdown['news_sentiment'] = round(ratio * 100, 1)
    score += news_contrib
    tvl_contrib = 0
    if tvl:
        change_7d = tvl.get('change_7d', 0) or 0
        if   change_7d >= 10:  tvl_contrib = 15
        elif change_7d >= 0:   tvl_contrib = 10
        elif change_7d >= -10: tvl_contrib = 5
        breakdown['tvl_7d'] = round(change_7d, 1)
        breakdown['tvl_usd'] = int(tvl.get('tvl', 0))
    else:
        rank = coin_meta.get('rank', 9999) or 9999
        if   rank <= 10:  tvl_contrib = 15
        elif rank <= 50:  tvl_contrib = 10
        elif rank <= 100: tvl_contrib = 5
    score += tvl_contrib
    return round(min(score, 100), 1), breakdown

def send_fundamentals_batch(batch):
    payload = {'secret': WEBHOOK_SECRET, 'coins': batch, 'timestamp': datetime.now(timezone.utc).isoformat()}
    try:
        r = requests.post(FUND_WEBHOOK_URL, json=payload, timeout=20)
        return r.status_code == 200
    except Exception as e:
        log.error(f"[FUND] Webhook error: {e}")
        return False

def _score_and_package_coin(c, tvl_map, tier):
    detail = fetch_coingecko_coin_detail(c['id'])
    tvl = tvl_map.get(c['symbol'])
    news = fetch_cryptopanic_sentiment(c['symbol']) if (CRYPTOPANIC_TOKEN and tier == 1) else None
    score, breakdown = calc_fundamental_score(c, detail, tvl, news)
    return {
        'symbol': c['symbol'], 'name': c['name'], 'rank': c['rank'],
        'mcap': c['mcap'], 'price': c['price'], 'score': score, 'breakdown': breakdown,
        'has_tvl': tvl is not None, 'tier': tier,
        'updated_at': datetime.now(timezone.utc).isoformat()
    }

def run_fundamentals_scan_tier(tier, coins, tvl_map, stream_batch_size=20):
    start = time.time()
    tier_name = {1: "Tier 1 (Top 50)", 2: "Tier 2 (51-200)", 3: "Tier 3 (201-500)"}[tier]
    log.info(f"📘 [Fundamentals] {tier_name} starting — {len(coins)} coins")
    buffer = []
    total_uploaded = 0
    for i, c in enumerate(coins):
        result = _score_and_package_coin(c, tvl_map, tier)
        if result:
            buffer.append(result)
        if len(buffer) >= stream_batch_size or (i == len(coins) - 1 and buffer):
            ok = send_fundamentals_batch(buffer)
            if ok:
                total_uploaded += len(buffer)
                log.info(f"[FUND] {tier_name} streamed {len(buffer)} coins → total {total_uploaded}/{len(coins)} ✅")
            buffer = []
        time.sleep(_cg_rate_sleep())
    elapsed = round((time.time() - start) / 60, 1)
    log.info(f"📘 [Fundamentals] {tier_name} done in {elapsed}m — {total_uploaded} uploaded")
    return total_uploaded

_last_tier1_run = 0
_last_tier2_run = 0
_last_tier3_run = 0

def run_fundamentals_scan():
    global _last_tier1_run, _last_tier2_run, _last_tier3_run
    start = time.time()
    log.info("")
    log.info("═══ 📘 Fundamentals scan cycle starting ═══")
    all_coins = fetch_coingecko_top_coins(FUND_COIN_LIMIT)
    if not all_coins:
        log.warning("[FUND] No coins returned, skipping cycle")
        return
    log.info(f"[FUND] Fetched {len(all_coins)} coins (target: top {FUND_COIN_LIMIT})")
    tvl_map = fetch_defillama_tvl()
    tier1 = all_coins[:30]
    tier2 = all_coins[30:100]
    tier3 = all_coins[100:250]
    now = time.time()
    if now - _last_tier1_run >= 2 * 3600 or _last_tier1_run == 0:
        run_fundamentals_scan_tier(1, tier1, tvl_map, stream_batch_size=10)
        _last_tier1_run = now
    else:
        mins_until = round((2 * 3600 - (now - _last_tier1_run)) / 60)
        log.info(f"[FUND] Tier 1 skipped (next run in ~{mins_until}m)")
    if now - _last_tier2_run >= 6 * 3600 or _last_tier2_run == 0:
        run_fundamentals_scan_tier(2, tier2, tvl_map, stream_batch_size=15)
        _last_tier2_run = now
    else:
        mins_until = round((6 * 3600 - (now - _last_tier2_run)) / 60)
        log.info(f"[FUND] Tier 2 skipped (next run in ~{mins_until}m)")
    if now - _last_tier3_run >= 12 * 3600 or _last_tier3_run == 0:
        run_fundamentals_scan_tier(3, tier3, tvl_map, stream_batch_size=20)
        _last_tier3_run = now
    else:
        mins_until = round((12 * 3600 - (now - _last_tier3_run)) / 60)
        log.info(f"[FUND] Tier 3 skipped (next run in ~{mins_until}m)")
    elapsed = round((time.time() - start) / 60, 1)
    log.info(f"═══ 📘 Fundamentals cycle done in {elapsed}m ═══")
    log.info("")

def fundamentals_loop():
    time.sleep(45)
    while True:
        try:
            run_fundamentals_scan()
        except Exception as e:
            log.error(f"[FUND] Cycle error: {e}")
            log.error(traceback.format_exc())
        time.sleep(15 * 60)

# ═══════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════
def main():
    import ccxt as ccxt_lib

    log.info("╔══════════════════════════════════════════════════════════╗")
    log.info("║      SignalEdge Triple-Engine Bot v5.3 (PATCHED)        ║")
    log.info("║  🤖 AI  +  🏦 Institutional  +  📊 Market Scanner      ║")
    log.info("║  v5.3: confidence 70→65, RSI veto + trend filter kept   ║")
    log.info("╚══════════════════════════════════════════════════════════╝")
    log.info(f"Scan interval: {SCAN_INTERVAL // 60} min · Workers: {MAX_WORKERS}")
    log.info("")

    exchange, exchange_name, valid_pairs = init_exchange_with_fallback(ccxt_lib)
    log.info("")

    fund_thread = threading.Thread(target=fundamentals_loop, daemon=True, name="fundamentals")
    fund_thread.start()
    cg_tier_label = "demo key (30/min)" if COINGECKO_API_KEY else "public (5-15/min)"
    log.info(f"📘 ProjectScore engine started — top-{FUND_COIN_LIMIT} coverage")
    log.info(f"   T1: 2h · T2: 6h · T3: 12h · CG: {cg_tier_label}")
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
