"""
SignalEdge Institutional Strategy Bot v3.0
==========================================
Multi-Timeframe Order Block Scanner
Scalping · Day Trading · Swing Trading

Timeframe Configuration:
  Scalping:   HTF=15m  LTF=1m   (quick flips, tight SL)
  Day Trade:  HTF=1h   LTF=5m   (intraday setups)
  Swing:      HTF=4h   LTF=1h   (multi-day moves)
  Position:   HTF=1d   LTF=4h   (weekly setups)

Author: SignalEdge
Version: 3.0.0
"""

import os, time, logging, requests, traceback
from datetime import datetime, timezone

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
log = logging.getLogger("SignalEdge")

# ─── CONFIG ───────────────────────────────────────
WEBHOOK_URL    = os.environ.get("WEBHOOK_URL",    "https://signaledge-server.onrender.com/webhook")
WEBHOOK_SECRET = os.environ.get("WEBHOOK_SECRET", "signaledge2025")
SCAN_INTERVAL  = int(os.environ.get("SCAN_INTERVAL_MINUTES", "5")) * 60
OB_LOOKBACK    = 6
SL_BUFFER_PCT  = 0.002
FIB1           = 1.618
FIB2           = 2.0
MIN_BODY_RATIO = 0.30
MIN_RR         = 1.0
MAX_RISK_PCT   = 5.0
COOLDOWN_HRS   = 2     # Per symbol per style

# ─── TRADING STYLES ───────────────────────────────
# Each style defines HTF (for OB detection) and label
STYLES = {
    "scalp": {
        "label":    "Scalping",
        "htf":      "15m",
        "candles":  150,
        "emoji":    "⚡",
        "hold":     "Minutes to 2 hours",
        "pairs":    50,    # Top 50 pairs only for scalp (more liquid)
    },
    "day": {
        "label":    "Day Trade",
        "htf":      "1h",
        "candles":  150,
        "emoji":    "📊",
        "hold":     "2–24 hours",
        "pairs":    100,   # Top 100 pairs
    },
    "swing": {
        "label":    "Swing Trade",
        "htf":      "4h",
        "candles":  150,
        "emoji":    "🎯",
        "hold":     "1–7 days",
        "pairs":    200,   # All 200 pairs
    },
    "position": {
        "label":    "Position Trade",
        "htf":      "1d",
        "candles":  150,
        "emoji":    "🏦",
        "hold":     "1–4 weeks",
        "pairs":    100,   # Top 100 pairs
    },
}

# ─── TOP 200 PAIRS ─────────────────────────────────
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

# ─── DEDUPLICATION ─────────────────────────────────
recent_signals: dict = {}

def already_signalled(symbol: str, style: str, direction: str) -> bool:
    key = f"{symbol}_{style}_{direction}"
    if key in recent_signals:
        elapsed = (datetime.now(timezone.utc) - recent_signals[key]).total_seconds()
        if elapsed < COOLDOWN_HRS * 3600:
            return True
    return False

def mark_signalled(symbol: str, style: str, direction: str):
    recent_signals[f"{symbol}_{style}_{direction}"] = datetime.now(timezone.utc)

# ─── ORDER BLOCK DETECTION ─────────────────────────
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
    # Bullish impulse → find last bearish candle = bull OB
    if c > prev[2] and c > o:
        for i in range(2, OB_LOOKBACK + 2):
            if i >= len(candles): break
            cd = candles[-i]
            if cd[4] < cd[1]:
                ob['bull_ob'] = {
                    'high': cd[2], 'low': cd[3],
                    'body_high': max(cd[1], cd[4]),
                    'body_low':  min(cd[1], cd[4])
                }
                break
    # Bearish impulse → find last bullish candle = bear OB
    if c < prev[3] and c < o:
        for i in range(2, OB_LOOKBACK + 2):
            if i >= len(candles): break
            cd = candles[-i]
            if cd[4] > cd[1]:
                ob['bear_ob'] = {
                    'high': cd[2], 'low': cd[3],
                    'body_high': max(cd[1], cd[4]),
                    'body_low':  min(cd[1], cd[4])
                }
                break
    return ob

# ─── ENTRY DETECTION ───────────────────────────────
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

    # Bullish flip/retest → LONG
    if 'bear_ob' in ob:
        ob_h = ob['bear_ob']['high']
        ob_l = ob['bear_ob']['low']
        if (prev_c <= ob_h and last_c > ob_h) or \
           (last_l <= ob_h and last_c > ob_h and prev_c > ob_h):
            return {'type': 'BUY', 'entry': last_c, 'ob_high': ob_h, 'ob_low': ob_l}

    # Bearish flip/retest → SHORT
    if 'bull_ob' in ob:
        ob_h = ob['bull_ob']['high']
        ob_l = ob['bull_ob']['low']
        if (prev_c >= ob_l and last_c < ob_l) or \
           (last_h >= ob_l and last_c < ob_l and prev_c < ob_l):
            return {'type': 'SELL', 'entry': last_c, 'ob_high': ob_h, 'ob_low': ob_l}

    return None

# ─── CALCULATE LEVELS ──────────────────────────────
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

# ─── SEND SIGNAL ───────────────────────────────────
def send_signal(symbol: str, sig_type: str, entry: float, levels: dict,
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
            log.info(
                f"  ✅ {style_cfg['emoji']} [{style_cfg['label'].upper()}] "
                f"{symbol} {sig_type} @ {entry:.6f} | "
                f"SL:{levels['sl']:.6f} | TP1:{levels['tp1']:.6f} | "
                f"RR:{levels['rr']} | TF:{timeframe}"
            )
            return True
        else:
            log.warning(f"  ⚠️ Webhook {r.status_code} for {symbol} [{style}]")
    except Exception as e:
        log.error(f"  ❌ Webhook error {symbol} [{style}]: {e}")
    return False

# ─── FETCH WITH RETRY ──────────────────────────────
def fetch_ohlcv(exchange, symbol: str, timeframe: str, limit: int = 150):
    for attempt in range(3):
        try:
            raw = exchange.fetch_ohlcv(symbol, timeframe, limit=limit)
            if raw and len(raw) >= 30:
                return raw
            return None
        except Exception as e:
            if 'BadSymbol' in str(type(e).__name__) or attempt == 2:
                return None
            time.sleep(2 ** attempt)
    return None

# ─── SCAN ONE STYLE ────────────────────────────────
def scan_style(exchange, valid_pairs: list, style: str, style_cfg: dict) -> int:
    htf     = style_cfg['htf']
    max_p   = style_cfg['pairs']
    pairs   = valid_pairs[:max_p]
    fired   = 0

    log.info(f"  {style_cfg['emoji']} [{style_cfg['label']}] Scanning {len(pairs)} pairs on {htf}...")

    for symbol in pairs:
        try:
            candles = fetch_ohlcv(exchange, symbol, htf, limit=150)
            if not candles:
                time.sleep(0.2)
                continue

            entry_sig = detect_entry(candles)
            if entry_sig:
                st = entry_sig['type']
                if not already_signalled(symbol, style, st):
                    lvls = calculate_levels(
                        entry_sig['entry'],
                        entry_sig['ob_high'],
                        entry_sig['ob_low'],
                        st
                    )
                    if lvls['rr'] >= MIN_RR and lvls['risk_pct'] <= MAX_RISK_PCT:
                        if send_signal(symbol, st, entry_sig['entry'], lvls, style, style_cfg, htf):
                            mark_signalled(symbol, style, st)
                            fired += 1

            time.sleep(0.25)

        except Exception as e:
            log.debug(f"  Error {symbol} [{style}]: {e}")
            continue

    log.info(f"  {style_cfg['emoji']} [{style_cfg['label']}] Done — {fired} signals fired")
    return fired

# ─── FULL SCAN ─────────────────────────────────────
def run_scan(exchange, valid_pairs: list):
    scan_time = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    log.info("")
    log.info("═" * 62)
    log.info(f"🤖 SignalEdge Multi-Timeframe Scan — {scan_time}")
    log.info(f"   Styles: Scalp · Day Trade · Swing · Position")
    log.info(f"   Pairs: {len(valid_pairs)} validated on Binance")
    log.info("═" * 62)

    total = 0
    for style, cfg in STYLES.items():
        try:
            fired = scan_style(exchange, valid_pairs, style, cfg)
            total += fired
        except Exception as e:
            log.error(f"Error in {style} scan: {e}")
            log.error(traceback.format_exc())

    log.info("")
    log.info(f"✅ Scan complete — {total} total signals fired across all styles")
    log.info(f"⏰ Next scan in {SCAN_INTERVAL // 60} minutes")
    log.info("")
    return total

# ─── VALIDATE PAIRS ────────────────────────────────
def get_valid_pairs(exchange) -> list:
    try:
        markets = exchange.load_markets()
        valid = [p for p in PAIRS if p in markets]
        log.info(f"📋 Valid pairs on Binance: {len(valid)}/{len(PAIRS)}")
        return valid
    except Exception as e:
        log.error(f"Could not load markets: {e}")
        return PAIRS[:50]

# ─── MAIN ──────────────────────────────────────────
def main():
    import ccxt as ccxt_lib

    log.info("╔══════════════════════════════════════════════════════════╗")
    log.info("║   SignalEdge Institutional Strategy Bot v3.0             ║")
    log.info("║   Multi-Timeframe Order Block Scanner                    ║")
    log.info("║   Scalping · Day Trading · Swing · Position              ║")
    log.info("╚══════════════════════════════════════════════════════════╝")
    log.info(f"Webhook:  {WEBHOOK_URL}")
    log.info(f"Interval: {SCAN_INTERVAL // 60} minutes")
    log.info(f"Styles:   {', '.join(STYLES.keys())}")
    log.info("")

    exchange = ccxt_lib.binance({'enableRateLimit': True})
    valid_pairs = get_valid_pairs(exchange)

    # First scan immediately on startup
    run_scan(exchange, valid_pairs)

    while True:
        time.sleep(SCAN_INTERVAL)
        try:
            run_scan(exchange, valid_pairs)
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
