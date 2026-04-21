"""
SignalEdge Institutional Strategy Bot v2
Zero heavy dependencies - uses only ccxt and requests
"""
import os, time, logging, requests, traceback
from datetime import datetime, timezone

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
log = logging.getLogger("SignalEdge")

# CONFIG
WEBHOOK_URL    = os.environ.get("WEBHOOK_URL", "https://signaledge-server.onrender.com/webhook")
WEBHOOK_SECRET = os.environ.get("WEBHOOK_SECRET", "signaledge2025")
SCAN_INTERVAL  = int(os.environ.get("SCAN_INTERVAL_MINUTES", "15")) * 60
HTF            = "4h"
OB_LOOKBACK    = 6
SL_BUFFER_PCT  = 0.002
FIB1           = 1.618
FIB2           = 2.0

# TOP 200 PAIRS
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
    "DREP/USDT","DUSK/USDT","EDO/USDT","EPIK/USDT","ETHW/USDT",
    "FOR/USDT","FORTH/USDT","FRONT/USDT","HARD/USDT","IRIS/USDT",
    "LAZIO/USDT","LINA/USDT","LUNA/USDT","NULS/USDT","OG/USDT",
    "REEF/USDT","REN/USDT","SKL/USDT","ALCX/USDT","AUCTION/USDT",
    "CREAM/USDT","CONV/USDT","COS/USDT","DREP/USDT","FORTH/USDT",
    "HIGH/USDT","ID/USDT","JASMY/USDT","JOE/USDT","NEXO/USDT",
]

# Dedup
recent_signals = {}

def already_signalled(symbol, signal_type, cooldown_h=4):
    key = f"{symbol}_{signal_type}"
    if key in recent_signals:
        elapsed = (datetime.now(timezone.utc) - recent_signals[key]).total_seconds()
        if elapsed < cooldown_h * 3600:
            return True
    return False

def mark_signalled(symbol, signal_type):
    recent_signals[f"{symbol}_{signal_type}"] = datetime.now(timezone.utc)

def detect_order_block(candles, lookback=OB_LOOKBACK):
    """candles = list of [ts, open, high, low, close, vol]"""
    if len(candles) < lookback + 5:
        return {}
    last = candles[-1]
    prev = candles[-2]
    o, h, l, c = last[1], last[2], last[3], last[4]
    body = abs(c - o)
    rng  = h - l
    if rng == 0 or body / rng < 0.3:
        return {}
    ob = {}
    # Bullish impulse → find last bearish candle
    if c > prev[2] and c > o:
        for i in range(2, lookback + 2):
            if i >= len(candles): break
            cd = candles[-i]
            if cd[4] < cd[1]:  # bearish
                ob['bull_ob'] = {'high': cd[2], 'low': cd[3],
                                  'body_high': max(cd[1],cd[4]), 'body_low': min(cd[1],cd[4])}
                break
    # Bearish impulse → find last bullish candle
    if c < prev[3] and c < o:
        for i in range(2, lookback + 2):
            if i >= len(candles): break
            cd = candles[-i]
            if cd[4] > cd[1]:  # bullish
                ob['bear_ob'] = {'high': cd[2], 'low': cd[3],
                                  'body_high': max(cd[1],cd[4]), 'body_low': min(cd[1],cd[4])}
                break
    return ob

def detect_entry(candles):
    """Detect flip + retest in one pass"""
    if len(candles) < 30:
        return None
    ob = detect_order_block(candles)
    if not ob:
        return None
    last_c = candles[-1][4]
    last_h = candles[-1][2]
    last_l = candles[-1][3]
    prev_c = candles[-2][4]

    # Bullish flip then retest → LONG
    if 'bear_ob' in ob:
        ob_h = ob['bear_ob']['high']
        ob_l = ob['bear_ob']['low']
        if prev_c <= ob_h and last_c > ob_h:
            return {'type': 'BUY', 'entry': last_c, 'ob_high': ob_h, 'ob_low': ob_l}
        if last_l <= ob_h and last_c > ob_h and prev_c > ob_h:
            return {'type': 'BUY', 'entry': last_c, 'ob_high': ob_h, 'ob_low': ob_l}

    # Bearish flip then retest → SHORT
    if 'bull_ob' in ob:
        ob_h = ob['bull_ob']['high']
        ob_l = ob['bull_ob']['low']
        if prev_c >= ob_l and last_c < ob_l:
            return {'type': 'SELL', 'entry': last_c, 'ob_high': ob_h, 'ob_low': ob_l}
        if last_h >= ob_l and last_c < ob_l and prev_c < ob_l:
            return {'type': 'SELL', 'entry': last_c, 'ob_high': ob_h, 'ob_low': ob_l}

    return None

def calculate_levels(entry, ob_high, ob_low, sig_type):
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
    return {'sl': round(sl,8), 'tp1': round(tp1,8), 'tp2': round(tp2,8),
            'tp3': round(tp3,8), 'risk_pct': round(abs(entry-sl)/entry*100,2), 'rr': rr}

def send_signal(symbol, sig_type, entry, levels):
    payload = {
        'secret': WEBHOOK_SECRET, 'type': sig_type,
        'symbol': symbol.replace('/USDT','USDT'),
        'price': entry, 'sl': levels['sl'],
        'tp1': levels['tp1'], 'tp2': levels['tp2'], 'tp3': levels['tp3'],
        'timeframe': HTF, 'strategy': 'SignalEdge Institutional',
        'risk_pct': levels['risk_pct'], 'rr': levels['rr'],
        'timestamp': datetime.now(timezone.utc).isoformat()
    }
    try:
        r = requests.post(WEBHOOK_URL, json=payload, timeout=8)
        if r.status_code == 200:
            log.info(f"✅ {symbol} {sig_type} @ {entry:.6f} | SL:{levels['sl']:.6f} | TP1:{levels['tp1']:.6f} | RR:{levels['rr']}")
            return True
        else:
            log.warning(f"⚠️ Webhook {r.status_code} for {symbol}")
    except Exception as e:
        log.error(f"❌ Webhook error {symbol}: {e}")
    return False

def scan_all(exchange, pairs):
    log.info(f"{'='*55}")
    log.info(f"🤖 SignalEdge Scan — {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    log.info(f"   Pairs: {len(pairs)} | TF: {HTF}")
    log.info(f"{'='*55}")
    fired = 0
    for i, symbol in enumerate(pairs, 1):
        try:
            candles = exchange.fetch_ohlcv(symbol, HTF, limit=100)
            if not candles or len(candles) < 30:
                time.sleep(0.2)
                continue
            entry_sig = detect_entry(candles)
            if entry_sig:
                st = entry_sig['type']
                if not already_signalled(symbol, st):
                    lvls = calculate_levels(entry_sig['entry'], entry_sig['ob_high'], entry_sig['ob_low'], st)
                    if lvls['rr'] >= 1.0 and lvls['risk_pct'] <= 5.0:
                        if send_signal(symbol, st, entry_sig['entry'], lvls):
                            mark_signalled(symbol, st)
                            fired += 1
            time.sleep(0.3)
        except Exception as e:
            log.debug(f"Error {symbol}: {e}")
            continue
        if i % 25 == 0:
            log.info(f"   Progress: {i}/{len(pairs)} | Signals: {fired}")
    log.info(f"✅ Done — {fired} signals | Next scan in {SCAN_INTERVAL//60}m\n")
    return fired

def main():
    import ccxt as ccxt_lib
    log.info("╔══════════════════════════════════════════════╗")
    log.info("║   SignalEdge Institutional Bot v2.0          ║")
    log.info("║   Order Block & Fibonacci Target Scanner     ║")
    log.info("╚══════════════════════════════════════════════╝")
    log.info(f"Webhook: {WEBHOOK_URL}")
    log.info(f"Interval: {SCAN_INTERVAL//60} min | Pairs: {len(PAIRS)}\n")

    exchange = ccxt_lib.binance({'enableRateLimit': True})

    # Validate pairs
    try:
        markets = exchange.load_markets()
        valid = [p for p in PAIRS if p in markets]
        log.info(f"Valid pairs: {len(valid)}/{len(PAIRS)}")
    except Exception as e:
        log.error(f"Could not load markets: {e}")
        valid = PAIRS[:50]

    scan_all(exchange, valid)

    while True:
        time.sleep(SCAN_INTERVAL)
        try:
            scan_all(exchange, valid)
        except KeyboardInterrupt:
            log.info("🛑 Stopped")
            break
        except Exception as e:
            log.error(f"Loop error: {e}\n{traceback.format_exc()}")
            time.sleep(60)

if __name__ == "__main__":
    main()
