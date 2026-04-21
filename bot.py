"""
SignalEdge Institutional Strategy Bot
======================================
Production-grade Order Block scanner for top 200 crypto pairs.
Detects institutional supply/demand zones, flips and retest entries.
Fires real-time webhooks to SignalEdge dashboard.

Strategy Logic:
  1. Identify Order Blocks (last opposing candle before impulsive move)
  2. Detect Flip (price closes beyond OB on HTF)
  3. Detect Retest Entry (price returns to flipped OB and respects it)
  4. Calculate SL (OB extreme + buffer) and TP1/TP2/TP3 (Fibonacci extensions)

Author: SignalEdge
Version: 2.0.0
"""

import os
import time
import json
import logging
import requests
import traceback
from datetime import datetime, timezone
from typing import Optional

import ccxt
import numpy as np
import pandas as pd

# ─────────────────────────────────────────
# LOGGING — production-grade
# ─────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S UTC",
    handlers=[
        logging.StreamHandler()
    ]
)
log = logging.getLogger("SignalEdgeBot")

# ─────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────
WEBHOOK_URL    = os.environ.get("WEBHOOK_URL", "https://signaledge-server.onrender.com/webhook")
WEBHOOK_SECRET = os.environ.get("WEBHOOK_SECRET", "signaledge2025")
SCAN_INTERVAL  = int(os.environ.get("SCAN_INTERVAL_MINUTES", "15")) * 60

# Order Block parameters (mirrors your Pine Script settings)
HTF             = "4h"     # Higher timeframe for OB detection
LTF             = "1h"     # Lower timeframe for retest confirmation
OB_LOOKBACK     = 6        # Max candles to look back for last opposing candle
SL_BUFFER_PCT   = 0.002    # 0.2% beyond OB extreme for stop loss
FIB1            = 1.618    # TP2 Fibonacci extension
FIB2            = 2.0      # TP3 Fibonacci extension
MAX_BARS_WAIT   = 25       # Max bars to wait for retest after flip
MIN_BODY_PCT    = 0.3      # Minimum candle body to OB ratio (displacement filter)
CANDLES_NEEDED  = 150      # Candles to fetch per symbol

# ─────────────────────────────────────────
# TOP 200 CRYPTO PAIRS BY MARKET CAP
# ─────────────────────────────────────────
TOP_200_PAIRS = [
    # Tier 1 — Top 20 (highest liquidity)
    "BTC/USDT","ETH/USDT","BNB/USDT","SOL/USDT","XRP/USDT",
    "DOGE/USDT","ADA/USDT","AVAX/USDT","SHIB/USDT","TRX/USDT",
    "DOT/USDT","LINK/USDT","MATIC/USDT","TON/USDT","UNI/USDT",
    "LTC/USDT","BCH/USDT","NEAR/USDT","ICP/USDT","APT/USDT",

    # Tier 2 — Top 21-60
    "FIL/USDT","ARB/USDT","OP/USDT","INJ/USDT","ATOM/USDT",
    "VET/USDT","GRT/USDT","ALGO/USDT","EGLD/USDT","XLM/USDT",
    "MANA/USDT","SAND/USDT","AXS/USDT","ENJ/USDT","THETA/USDT",
    "FLOW/USDT","XTZ/USDT","EOS/USDT","IOTA/USDT","NEO/USDT",
    "KAVA/USDT","ZIL/USDT","ONE/USDT","SKL/USDT","CELO/USDT",
    "ROSE/USDT","ANKR/USDT","SUI/USDT","SEI/USDT","TIA/USDT",
    "STX/USDT","RNDR/USDT","FET/USDT","AGIX/USDT","OCEAN/USDT",
    "WLD/USDT","CFX/USDT","BLUR/USDT","ID/USDT","HIGH/USDT",

    # Tier 3 — Top 61-120
    "BAND/USDT","CKB/USDT","CTSI/USDT","DYDX/USDT","ENS/USDT",
    "FLOKI/USDT","GALA/USDT","GMT/USDT","HBAR/USDT","HOT/USDT",
    "IMX/USDT","JASMY/USDT","JOE/USDT","KSM/USDT","LINA/USDT",
    "LRC/USDT","MAGIC/USDT","MASK/USDT","MDT/USDT","MINA/USDT",
    "MTL/USDT","NEXO/USDT","NKN/USDT","NULS/USDT","OG/USDT",
    "OGN/USDT","OMG/USDT","ONT/USDT","PEPE/USDT","PEOPLE/USDT",
    "QNT/USDT","QTUM/USDT","RAY/USDT","REEF/USDT","REN/USDT",
    "REQ/USDT","RLC/USDT","RUNE/USDT","RVN/USDT","SC/USDT",
    "SNX/USDT","SPELL/USDT","STORJ/USDT","SUPER/USDT","SXP/USDT",
    "SYS/USDT","TFUEL/USDT","TOMO/USDT","TWT/USDT","UFT/USDT",
    "UNFI/USDT","UTK/USDT","VGX/USDT","VOXEL/USDT","WAN/USDT",
    "WAVES/USDT","WIN/USDT","WOO/USDT","XEM/USDT","XMR/USDT",

    # Tier 4 — Top 121-200
    "XNO/USDT","XVG/USDT","YFI/USDT","YGG/USDT","ZEC/USDT",
    "ZEN/USDT","ZRX/USDT","1INCH/USDT","AAVE/USDT","ACH/USDT",
    "ACM/USDT","ADA/USDT","AERGO/USDT","AGLD/USDT","AKRO/USDT",
    "ALCX/USDT","ALPHA/USDT","AMP/USDT","ANCT/USDT","ARPA/USDT",
    "ASR/USDT","AUCTION/USDT","AUTO/USDT","AVA/USDT","BADGER/USDT",
    "BAL/USDT","BAKE/USDT","BETA/USDT","BLZ/USDT","BNT/USDT",
    "BSW/USDT","C98/USDT","CAKE/USDT","CHR/USDT","CHZ/USDT",
    "CLV/USDT","COMP/USDT","CONV/USDT","COS/USDT","COTI/USDT",
    "CREAM/USDT","CRV/USDT","CVC/USDT","CVP/USDT","CVX/USDT",
    "DAR/USDT","DATA/USDT","DENT/USDT","DGB/USDT","DOCK/USDT",
    "DREP/USDT","DUSK/USDT","DYDX/USDT","EDO/USDT","ELF/USDT",
    "EPIK/USDT","ERN/USDT","ETHW/USDT","FARM/USDT","FOR/USDT",
    "FORTH/USDT","FRONT/USDT","FTM/USDT","FUN/USDT","GHST/USDT",
    "GLM/USDT","GLMR/USDT","GNO/USDT","HARD/USDT","HIFI/USDT",
    "ILV/USDT","IRIS/USDT","IOST/USDT","IOTX/USDT","LAZIO/USDT",
    "LIT/USDT","LOKA/USDT","LOOM/USDT","LPT/USDT","LUNA/USDT",
]

# ─────────────────────────────────────────
# SIGNAL DEDUPLICATION — prevent spam
# ─────────────────────────────────────────
recent_signals: dict = {}   # symbol -> last signal timestamp

def already_signalled(symbol: str, signal_type: str, cooldown_hours: int = 4) -> bool:
    key = f"{symbol}_{signal_type}"
    if key in recent_signals:
        elapsed = (datetime.now(timezone.utc) - recent_signals[key]).total_seconds()
        if elapsed < cooldown_hours * 3600:
            return True
    return False

def mark_signalled(symbol: str, signal_type: str):
    key = f"{symbol}_{signal_type}"
    recent_signals[key] = datetime.now(timezone.utc)

# ─────────────────────────────────────────
# ORDER BLOCK DETECTION
# ─────────────────────────────────────────
def detect_order_block(df: pd.DataFrame, lookback: int = OB_LOOKBACK) -> dict:
    """
    Identifies the last opposing candle before an impulsive move.
    A bullish OB = last bearish candle before a bullish impulse.
    A bearish OB = last bullish candle before a bearish impulse.
    """
    if len(df) < lookback + 5:
        return {}

    last = df.iloc[-1]
    prev = df.iloc[-2]

    # Displacement: last closed candle must be impulsive
    body_size = abs(last['close'] - last['open'])
    range_size = last['high'] - last['low']
    if range_size == 0:
        return {}
    body_ratio = body_size / range_size

    if body_ratio < MIN_BODY_PCT:
        return {}

    ob = {}

    # Bullish impulse: close > previous high → look for last bearish candle
    if last['close'] > prev['high'] and last['close'] > last['open']:
        for i in range(2, lookback + 2):
            if i >= len(df):
                break
            candle = df.iloc[-i]
            if candle['close'] < candle['open']:  # bearish candle
                ob['bull_ob'] = {
                    'high': float(candle['high']),
                    'low':  float(candle['low']),
                    'body_high': float(max(candle['open'], candle['close'])),
                    'body_low':  float(min(candle['open'], candle['close'])),
                    'index': len(df) - i
                }
                break

    # Bearish impulse: close < previous low → look for last bullish candle
    if last['close'] < prev['low'] and last['close'] < last['open']:
        for i in range(2, lookback + 2):
            if i >= len(df):
                break
            candle = df.iloc[-i]
            if candle['close'] > candle['open']:  # bullish candle
                ob['bear_ob'] = {
                    'high': float(candle['high']),
                    'low':  float(candle['low']),
                    'body_high': float(max(candle['open'], candle['close'])),
                    'body_low':  float(min(candle['open'], candle['close'])),
                    'index': len(df) - i
                }
                break

    return ob

# ─────────────────────────────────────────
# FLIP DETECTION
# ─────────────────────────────────────────
def detect_flip(df_htf: pd.DataFrame) -> Optional[dict]:
    """
    Detects when price closes BEYOND an order block — the flip.
    Bullish flip: price was in bull OB zone, now closes below OB low.
    Bearish flip: price was in bear OB zone, now closes above OB high.
    """
    if len(df_htf) < 30:
        return None

    ob = detect_order_block(df_htf)
    if not ob:
        return None

    last_close = float(df_htf.iloc[-1]['close'])
    prev_close = float(df_htf.iloc[-2]['close'])

    # Bearish flip: close below bullish OB → expect short
    if 'bull_ob' in ob:
        ob_low = ob['bull_ob']['low']
        ob_high = ob['bull_ob']['high']
        if prev_close >= ob_low and last_close < ob_low:
            return {
                'type': 'flip_short',
                'ob_high': ob_high,
                'ob_low': ob_low,
                'flip_level': ob_low,
                'close': last_close
            }

    # Bullish flip: close above bearish OB → expect long
    if 'bear_ob' in ob:
        ob_low = ob['bear_ob']['low']
        ob_high = ob['bear_ob']['high']
        if prev_close <= ob_high and last_close > ob_high:
            return {
                'type': 'flip_long',
                'ob_high': ob_high,
                'ob_low': ob_low,
                'flip_level': ob_high,
                'close': last_close
            }

    return None

# ─────────────────────────────────────────
# RETEST ENTRY DETECTION
# ─────────────────────────────────────────
def detect_retest(df_htf: pd.DataFrame, flip: dict) -> Optional[dict]:
    """
    After a flip, waits for price to return to the OB zone (retest)
    and then close back in the direction of the flip — the entry.
    """
    if not flip or len(df_htf) < 5:
        return None

    last = df_htf.iloc[-1]
    last_close = float(last['close'])
    last_high  = float(last['high'])
    last_low   = float(last['low'])

    ob_high = flip['ob_high']
    ob_low  = flip['ob_low']

    if flip['type'] == 'flip_short':
        # Retest: price wicks up into OB zone but closes below ob_low
        if last_high >= ob_low and last_close < ob_low:
            return {
                'type': 'SELL',
                'entry': last_close,
                'ob_high': ob_high,
                'ob_low': ob_low,
            }

    elif flip['type'] == 'flip_long':
        # Retest: price wicks down into OB zone but closes above ob_high
        if last_low <= ob_high and last_close > ob_high:
            return {
                'type': 'BUY',
                'entry': last_close,
                'ob_high': ob_high,
                'ob_low': ob_low,
            }

    return None

# ─────────────────────────────────────────
# CALCULATE STOP LOSS & TARGETS
# ─────────────────────────────────────────
def calculate_levels(entry: float, ob_high: float, ob_low: float, signal_type: str) -> dict:
    """
    SL: OB extreme + buffer
    TP1: 1:1 RR
    TP2: 1.618 Fibonacci extension
    TP3: 2.0 Fibonacci extension
    """
    if signal_type == 'BUY':
        sl = ob_low * (1 - SL_BUFFER_PCT)
        risk = entry - sl
        tp1 = entry + risk
        tp2 = entry + (risk * FIB1)
        tp3 = entry + (risk * FIB2)
    else:  # SELL
        sl = ob_high * (1 + SL_BUFFER_PCT)
        risk = sl - entry
        tp1 = entry - risk
        tp2 = entry - (risk * FIB1)
        tp3 = entry - (risk * FIB2)

    rr_ratio = round(abs(tp1 - entry) / abs(entry - sl), 2) if abs(entry - sl) > 0 else 0

    return {
        'sl':    round(sl, 8),
        'tp1':   round(tp1, 8),
        'tp2':   round(tp2, 8),
        'tp3':   round(tp3, 8),
        'risk_pct': round(abs(entry - sl) / entry * 100, 2),
        'rr':    rr_ratio
    }

# ─────────────────────────────────────────
# SEND WEBHOOK
# ─────────────────────────────────────────
def send_signal(symbol: str, signal_type: str, entry: float, levels: dict, timeframe: str = HTF):
    """Send confirmed signal to SignalEdge dashboard"""
    payload = {
        'secret':    WEBHOOK_SECRET,
        'type':      signal_type,
        'symbol':    symbol.replace('/USDT', 'USDT'),
        'price':     entry,
        'sl':        levels['sl'],
        'tp1':       levels['tp1'],
        'tp2':       levels['tp2'],
        'tp3':       levels['tp3'],
        'timeframe': timeframe,
        'strategy':  'SignalEdge Institutional',
        'risk_pct':  levels['risk_pct'],
        'rr':        levels['rr'],
        'timestamp': datetime.now(timezone.utc).isoformat()
    }

    try:
        resp = requests.post(WEBHOOK_URL, json=payload, timeout=8)
        if resp.status_code == 200:
            log.info(f"  ✅ SIGNAL SENT → {symbol} {signal_type} @ {entry:.6f} | SL: {levels['sl']:.6f} | TP1: {levels['tp1']:.6f} | RR: {levels['rr']}")
            return True
        else:
            log.warning(f"  ⚠️  Webhook returned {resp.status_code} for {symbol}")
            return False
    except requests.exceptions.Timeout:
        log.error(f"  ❌ Webhook timeout for {symbol}")
        return False
    except Exception as e:
        log.error(f"  ❌ Webhook error for {symbol}: {e}")
        return False

# ─────────────────────────────────────────
# FETCH OHLCV WITH RETRY
# ─────────────────────────────────────────
def fetch_ohlcv(exchange, symbol: str, timeframe: str, limit: int = CANDLES_NEEDED) -> Optional[pd.DataFrame]:
    """Fetch OHLCV with retry logic"""
    for attempt in range(3):
        try:
            raw = exchange.fetch_ohlcv(symbol, timeframe, limit=limit)
            if not raw or len(raw) < 30:
                return None
            df = pd.DataFrame(raw, columns=['timestamp','open','high','low','close','volume'])
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms', utc=True)
            return df
        except ccxt.BadSymbol:
            return None
        except Exception as e:
            if attempt == 2:
                log.debug(f"Failed to fetch {symbol} {timeframe}: {e}")
                return None
            time.sleep(2 ** attempt)
    return None

# ─────────────────────────────────────────
# SCAN ONE SYMBOL
# ─────────────────────────────────────────
def scan_symbol(exchange, symbol: str) -> int:
    """Scan a single symbol for institutional signals. Returns number of signals fired."""
    signals_fired = 0

    # Fetch HTF data
    df_htf = fetch_ohlcv(exchange, symbol, HTF)
    if df_htf is None:
        return 0

    # 1. Check for flip on HTF
    flip = detect_flip(df_htf)

    # 2. Check for retest entry on HTF
    if flip:
        retest = detect_retest(df_htf, flip)
        if retest:
            signal_type = retest['type']

            # Deduplication check
            if already_signalled(symbol, signal_type):
                return 0

            entry  = retest['entry']
            levels = calculate_levels(entry, retest['ob_high'], retest['ob_low'], signal_type)

            # Quality filter: minimum 1:1 RR
            if levels['rr'] < 1.0:
                return 0

            # Quality filter: max 5% risk
            if levels['risk_pct'] > 5.0:
                return 0

            sent = send_signal(symbol, signal_type, entry, levels)
            if sent:
                mark_signalled(symbol, signal_type)
                signals_fired += 1

    return signals_fired

# ─────────────────────────────────────────
# VALIDATE PAIRS AGAINST EXCHANGE
# ─────────────────────────────────────────
def get_valid_pairs(exchange) -> list:
    """Filter TOP_200_PAIRS to only those available on Binance"""
    try:
        markets = exchange.load_markets()
        valid = [p for p in TOP_200_PAIRS if p in markets]
        log.info(f"📋 Valid pairs on Binance: {len(valid)}/{len(TOP_200_PAIRS)}")
        return valid
    except Exception as e:
        log.error(f"Could not load markets: {e}")
        return TOP_200_PAIRS[:50]

# ─────────────────────────────────────────
# MAIN SCAN LOOP
# ─────────────────────────────────────────
def run_scan(exchange, pairs: list):
    """Full scan across all pairs"""
    scan_time = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    log.info(f"")
    log.info(f"{'═'*60}")
    log.info(f"🤖 SignalEdge Institutional Scan — {scan_time}")
    log.info(f"   Pairs: {len(pairs)} | Timeframe: {HTF} | Strategy: Order Block & Fib")
    log.info(f"{'═'*60}")

    total_signals = 0
    errors = 0

    for i, symbol in enumerate(pairs, 1):
        try:
            signals = scan_symbol(exchange, symbol)
            total_signals += signals
            time.sleep(0.3)  # Respect rate limits
        except Exception as e:
            errors += 1
            log.debug(f"Error on {symbol}: {e}")
            continue

        # Progress update every 25 pairs
        if i % 25 == 0:
            log.info(f"   Progress: {i}/{len(pairs)} pairs scanned | Signals: {total_signals}")

    log.info(f"")
    log.info(f"✅ Scan complete — {total_signals} signals fired | {errors} errors | {len(pairs)} pairs scanned")
    log.info(f"⏰ Next scan in {SCAN_INTERVAL//60} minutes")
    log.info(f"")

    return total_signals

# ─────────────────────────────────────────
# STARTUP
# ─────────────────────────────────────────
def main():
    log.info("╔══════════════════════════════════════════════════════╗")
    log.info("║     SignalEdge Institutional Strategy Bot v2.0       ║")
    log.info("║     Order Block & Fibonacci Target Detection         ║")
    log.info("╚══════════════════════════════════════════════════════╝")
    log.info(f"")
    log.info(f"  Webhook:  {WEBHOOK_URL}")
    log.info(f"  Interval: {SCAN_INTERVAL//60} minutes")
    log.info(f"  HTF:      {HTF}")
    log.info(f"  Pairs:    Up to {len(TOP_200_PAIRS)}")
    log.info(f"")

    # Init exchange
    exchange = ccxt.binance({'enableRateLimit': True})

    # Validate pairs
    valid_pairs = get_valid_pairs(exchange)

    # Run first scan immediately
    run_scan(exchange, valid_pairs)

    # Then run on schedule
    while True:
        time.sleep(SCAN_INTERVAL)
        try:
            run_scan(exchange, valid_pairs)
        except KeyboardInterrupt:
            log.info("🛑 Bot stopped by user")
            break
        except Exception as e:
            log.error(f"Unexpected error in main loop: {e}")
            log.error(traceback.format_exc())
            log.info("Restarting in 60 seconds...")
            time.sleep(60)

if __name__ == "__main__":
    main()
