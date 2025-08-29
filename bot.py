# bot.py — v2.7.0 ws-kline-monitor

import os, csv, hmac, hashlib, json, threading, time, re
from decimal import Decimal, ROUND_DOWN
from collections import OrderedDict, defaultdict
from datetime import datetime, timezone
from flask import Flask, request, jsonify

# ==== Binance (USDT-M Futures) ====
BINANCE_ENABLED = os.environ.get("TRADING_ENABLED", "false").lower() == "true"

UMFutures = None
UMFuturesWS = None
_BINANCE_IMPORT_PATH = None
_WS_IMPORT_PATH = None
if BINANCE_ENABLED:
    try:
        from binance.um_futures import UMFutures as _UM
        UMFutures = _UM
        _BINANCE_IMPORT_PATH = "binance.um_futures"
    except Exception as e1:
        try:
            from binance.lib.um_futures import UMFutures as _UM
            UMFutures = _UM
            _BINANCE_IMPORT_PATH = "binance.lib.um_futures"
        except Exception as e2:
            print("[WARN] Cannot import UMFutures; trading disabled:", repr(e1), "|", repr(e2), flush=True)
            BINANCE_ENABLED = False
            UMFutures = None

    # Websocket client (several SDK layouts supported)
    try:
        from binance.websocket.um_futures.websocket_client import UMFuturesWebsocketClient as _WS
        UMFuturesWS = _WS
        _WS_IMPORT_PATH = "binance.websocket.um_futures.UMFuturesWebsocketClient"
    except Exception as ews1:
        try:
            # older name
            from binance.websocket.um_futures import UMFuturesWebsocketClient as _WS
            UMFuturesWS = _WS
            _WS_IMPORT_PATH = "binance.websocket.um_futures"
        except Exception as ews2:
            UMFuturesWS = None
            _WS_IMPORT_PATH = None
            print("[WARN] UMFuturesWebsocketClient not available:", repr(ews1), "|", repr(ews2), flush=True)

app = Flask(__name__)

# ===== ENV / config =====
LOG_DIR   = os.environ.get("LOG_DIR", "logs")
LOG_FILE  = os.environ.get("LOG_FILE", "engulfing.csv")
TECH_LOG  = os.environ.get("TECH_LOG", "tech.jsonl")
EXEC_LOG  = os.environ.get("EXEC_LOG", "executions.csv")
SECRET    = os.environ.get("WEBHOOK_SECRET", "")
ALLOW_PATTERN = os.environ.get("ALLOW_PATTERN", "engulfing").lower()
ENV_MODE  = os.environ.get("ENV", "prod")
MAX_KEYS  = int(os.environ.get("DEDUP_CACHE", "2000"))
ROTATE_BYTES = int(os.environ.get("ROTATE_BYTES", str(5*1024*1024)))
KEEP_FILES   = int(os.environ.get("KEEP_FILES", "3"))

# Binance trade settings
TESTNET         = os.environ.get("TESTNET", "true").lower() == "true"
API_KEY_MAIN    = os.environ.get("BINANCE_API_KEY", "")
API_SECRET_MAIN = os.environ.get("BINANCE_API_SECRET", "")
API_KEY_TEST    = os.environ.get("BINANCE_API_KEY_TEST", "")
API_SECRET_TEST = os.environ.get("BINANCE_API_SECRET_TEST", "")

LEVERAGE     = int(os.environ.get("LEVERAGE", "10"))
RISK_MODE    = os.environ.get("RISK_MODE", "margin").lower()
RISK_PCT     = float(os.environ.get("RISK_PCT", "1.0"))
ADMIN_TOKEN  = os.environ.get("ADMIN_TOKEN", "")
PRESET_SYMBOLS = [x.strip().upper() for x in os.environ.get("PRESET_SYMBOLS","").split(",") if x.strip()]

# Bracket monitor settings
CANCEL_ORPHANS   = os.environ.get("CANCEL_ORPHANS", "true").lower() == "true"
BRACKET_POLL_SEC = float(os.environ.get("BRACKET_POLL_SEC", "1"))

# Інтервал для перевірки закритих свічок (SL-by-close)
KLINE_INTERVAL = os.environ.get("KLINE_INTERVAL", "1m")

# Single-position режим
REPLACE_ON_NEW = os.environ.get("REPLACE_ON_NEW", "false").lower() == "true"

os.makedirs(LOG_DIR, exist_ok=True)
CSV_PATH   = os.path.join(LOG_DIR, LOG_FILE)
TECH_PATH  = os.path.join(LOG_DIR, TECH_LOG)
EXEC_PATH  = os.path.join(LOG_DIR, EXEC_LOG)

# ===== utils =====
DEDUP = OrderedDict()
# локальні брекети: symbol -> {...}
BRACKETS = {}
BR_LOCK  = threading.RLock()

# ws кеш: symbol -> interval -> (o,h,l,c,close_time_ms)
KLINE_CACHE = defaultdict(dict)
WS_CLIENT = None
WS_STARTED = set()
WS_LOCK = threading.RLock()

def techlog(entry: dict):
    entry["ts"] = datetime.now(timezone.utc).isoformat().replace("+00:00","Z")
    os.makedirs(LOG_DIR, exist_ok=True)
    with open(TECH_PATH, "a", encoding="utf-8") as f:
        f.write(json.dumps(entry, ensure_ascii=False) + "\n")
    print(f"[TECH] {json.dumps(entry, ensure_ascii=False)}", flush=True)

def rotate_if_needed(path: str):
    try:
        if not os.path.exists(path) or os.path.getsize(path) < ROTATE_BYTES:
            return
        for i in range(KEEP_FILES, 0, -1):
            src = f"{path}.{i}"
            dst = f"{path}.{i+1}"
            if os.path.exists(src):
                if i == KEEP_FILES:
                    os.remove(src)
                else:
                    os.rename(src, dst)
        os.rename(path, f"{path}.1")
    except Exception as e:
        techlog({"level":"warn","msg":"rotate_failed","err":str(e),"path":path})

def _ensure_exec_header():
    if not os.path.exists(EXEC_PATH) or os.path.getsize(EXEC_PATH) == 0:
        with open(EXEC_PATH, "w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow(
                ["signal_id","event","time","price","qty","commission","commission_asset","realized_pnl","symbol","side","order_id"]
            )

def _exec_write_row(row):
    rotate_if_needed(EXEC_PATH)
    _ensure_exec_header()
    with open(EXEC_PATH, "a", newline="", encoding="utf-8") as f:
        csv.writer(f).writerow(row)

def exec_log(signal_id, event, iso_time, price, qty, commission, commission_asset, realized_pnl, symbol, side, order_id):
    _exec_write_row([signal_id, event, iso_time,
                     price if price is not None else "",
                     qty if qty is not None else "",
                     commission if commission is not None else "",
                     commission_asset or "",
                     realized_pnl if realized_pnl is not None else "",
                     symbol, side, order_id])

def to_float(x):
    try: return float(x)
    except Exception: return None

def to_iso8601(ts_str: str) -> str:
    s = str(ts_str).strip()
    try:
        v = float(s); iv = int(v)
        if iv > 10_000_000_000:
            return datetime.fromtimestamp(iv/1000, tz=timezone.utc).isoformat().replace("+00:00","Z")
        if iv > 1_000_000_000:
            return datetime.fromtimestamp(iv, tz=timezone.utc).isoformat().replace("+00:00","Z")
    except Exception:
        pass
    try:
        if s.endswith("Z"): s = s[:-1] + "+00:00"
        return datetime.fromisoformat(s).astimezone(timezone.utc).isoformat().replace("+00:00","Z")
    except Exception:
        return datetime.now(timezone.utc).isoformat().replace("+00:00","Z")

def calc_sig(raw_body: bytes) -> str:
    if not SECRET: return ""
    return hmac.new(SECRET.encode("utf-8"), raw_body, hashlib.sha256).hexdigest()

def valid_sig(req) -> bool:
    if not SECRET: return True
    header = req.headers.get("X-Signature", "")
    try:
        raw = req.get_data(cache=False, as_text=False)
    except Exception:
        return False
    return hmac.compare_digest(header, calc_sig(raw))

def validate_payload(d: dict):
    required = ["signal","symbol","time","side","pattern","entry","tp","sl"]
    miss = [k for k in required if k not in d]
    if miss: return False, f"Missing fields: {','.join(miss)}"
    if str(d["signal"]).lower() != "entry": return False, "signal must be 'entry'"
    if str(d["side"]).lower() not in ("long","short"): return False, "side must be long/short"
    if str(d["pattern"]).lower() != ALLOW_PATTERN: return False, f"pattern must be '{ALLOW_PATTERN}'"
    entry = to_float(d["entry"]); tp = to_float(d["tp"]); sl = to_float(d["sl"])
    if entry is None or tp is None or sl is None: return False, "entry/tp/sl must be numeric"
    return True, {"entry":entry, "tp":tp, "sl":sl}

def build_id(pattern: str, side: str, time_raw: str, entry_val: float) -> str:
    return f"{pattern}|{side}|{str(time_raw).lower()}|{('{:.10f}'.format(float(entry_val)))}"

def dedup_seen(key: str) -> bool:
    if key in DEDUP:
        DEDUP.move_to_end(key); return True
    DEDUP[key] = True
    if len(DEDUP) > MAX_KEYS: DEDUP.popitem(last=False)
    return False

# ===== heartbeat =====
def _heartbeat():
    while True:
        print(f"[HEARTBEAT] {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} alive", flush=True)
        time.sleep(60)
threading.Thread(target=_heartbeat, daemon=True).start()
print("[BOOT] bot process started", flush=True)

# ===== Binance helpers (One-way) =====
BINANCE = None
ONEWAY_SET = False
SYMBOL_CACHE = {}
LEVERAGE_SET = set()

def tv_to_binance_symbol(tv_symbol: str) -> str:
    return re.sub(r"\.P$", "", str(tv_symbol)).upper()

def q_floor_to_step(qty: float, step: float) -> float:
    if step <= 0: return qty
    return float((Decimal(str(qty)) / Decimal(str(step))).to_integral_value(rounding=ROUND_DOWN) * Decimal(str(step)))

def p_floor_to_tick(price: float, tick: float) -> float:
    if tick <= 0: return price
    mult = Decimal(str(price)) / Decimal(str(tick))
    return float((mult.to_integral_value(rounding=ROUND_DOWN)) * Decimal(str(tick)))

def fetch_symbol_filters(symbol: str):
    symbol = symbol.upper()
    if symbol in SYMBOL_CACHE: return SYMBOL_CACHE[symbol]
    info = BINANCE.exchange_info()
    filt = {"stepSize":None, "tickSize":None, "minQty":None, "minNotional":None}
    for s in info.get("symbols", []):
        if s.get("symbol") == symbol:
            for f in s.get("filters", []):
                t = f.get("filterType")
                if t == "LOT_SIZE":
                    filt["stepSize"] = float(f["stepSize"]); filt["minQty"] = float(f["minQty"])
                elif t == "PRICE_FILTER":
                    filt["tickSize"] = float(f["tickSize"])
                elif t in ("MIN_NOTIONAL","NOTIONAL"):
                    mn = f.get("notional") or f.get("minNotional")
                    if mn is not None: filt["minNotional"] = float(mn)
            SYMBOL_CACHE[symbol] = filt
            return filt
    raise ValueError(f"Symbol {symbol} not found in exchangeInfo")

def get_available_balance_usdt() -> float:
    bals = BINANCE.balance()
    for b in bals:
        if b.get("asset") == "USDT": return float(b.get("availableBalance"))
    raise RuntimeError("USDT balance not found")

def get_mark_price(symbol: str) -> float:
    mp = BINANCE.mark_price(symbol=symbol.upper())
    return float(mp["markPrice"])

def ensure_oneway_mode():
    global ONEWAY_SET
    if ONEWAY_SET: return
    try:
        try:
            st = BINANCE.get_position_mode()
            dual = str(st.get("dualSidePosition", "false")).lower() in ("true", "1")
            if not dual:
                ONEWAY_SET = True
                techlog({"level":"info","msg":"oneway_mode_ok","source":"precheck"})
                return
        except Exception:
            pass
        BINANCE.change_position_mode(dualSidePosition="false")
        ONEWAY_SET = True
        techlog({"level":"info","msg":"oneway_mode_ok"})
    except Exception as e:
        msg = str(e)
        if "-4059" in msg or "no need to change position side" in msg.lower() or "not modified" in msg.lower():
            ONEWAY_SET = True
            techlog({"level":"info","msg":"oneway_mode_already","detail":msg})
        else:
            techlog({"level":"warn","msg":"change_position_mode_failed","err":msg})

def ensure_leverage(symbol: str):
    symbol = symbol.upper()
    if symbol in LEVERAGE_SET: return
    try:
        BINANCE.change_leverage(symbol=symbol, leverage=LEVERAGE)
        LEVERAGE_SET.add(symbol)
        techlog({"level":"info","msg":"leverage_ok","symbol":symbol,"leverage":LEVERAGE})
    except Exception as e:
        techlog({"level":"warn","msg":"change_leverage_failed","symbol":symbol,"err":str(e)})

def _fetch_positions(symbol: str):
    if not BINANCE: return []
    methods = ["position_risk","get_position_risk","position_information","get_position_information"]
    for m in methods:
        if hasattr(BINANCE, m):
            try:
                data = getattr(BINANCE, m)(symbol=symbol)
                if data is None: continue
                if isinstance(data, dict) and "positions" in data: return data["positions"]
                if isinstance(data, list): return data
            except Exception:
                continue
    return []

def compute_qty(symbol: str, price: float) -> float:
    bal = get_available_balance_usdt()
    if RISK_MODE == "margin":
        margin = bal * (RISK_PCT / 100.0); notional = margin * LEVERAGE
    else:
        notional = bal * (RISK_PCT / 100.0)
    qty_raw = notional / price
    filt = fetch_symbol_filters(symbol)
    step = filt["stepSize"] or 0.001
    min_qty = filt["minQty"] or 0.0
    min_not = filt["minNotional"] or 0.0
    qty = q_floor_to_step(qty_raw, step)
    if min_qty and qty < min_qty: qty = min_qty
    if min_not and (qty * price) < min_not: qty = q_floor_to_step((min_not / price), step) or min_qty
    if qty <= 0: raise RuntimeError("Computed qty <= 0. Increase RISK_PCT or leverage.")
    return qty

def _get_order_status(symbol: str, order_id: int) -> str:
    try:
        od = BINANCE.get_order(symbol=symbol, orderId=order_id)
    except Exception:
        od = BINANCE.query_order(symbol=symbol, orderId=order_id)
    return str(od.get("status", "")).upper()

def _list_open_orders(symbol: str):
    for m in ("get_open_orders","open_orders"):
        if hasattr(BINANCE, m):
            try: return getattr(BINANCE, m)(symbol=symbol)
            except Exception: continue
    return []

# ===== Websocket kline handling =====
def _ws_kline_cb(_, msg):
    """
    Normalized handler: store LAST CLOSED candle in KLINE_CACHE.
    """
    try:
        d = msg.get("data") if isinstance(msg, dict) and "data" in msg else msg
        k = d.get("k") if isinstance(d, dict) else None
        if not k: return
        if not k.get("x"):  # only closed bars
            return
        sym = str(d.get("s") or k.get("s") or "").upper()
        itv = str(k.get("i"))
        o = float(k.get("o")); h = float(k.get("h")); l = float(k.get("l")); c = float(k.get("c"))
        ct = int(k.get("T"))  # close time ms
        with WS_LOCK:
            KLINE_CACHE[sym][itv] = (o,h,l,c,ct)
        techlog({"level":"debug","msg":"ws_kline_closed_bar","symbol":sym,"interval":itv,"close":c})
    except Exception as e:
        techlog({"level":"warn","msg":"ws_kline_parse_failed","err":str(e)})

def _start_ws_kline(symbol: str, interval: str):
    if not UMFuturesWS: 
        techlog({"level":"warn","msg":"ws_not_available"})
        return
    sym = symbol.upper()
    with WS_LOCK:
        key = f"{sym}|{interval}"
        if key in WS_STARTED: 
            return
        try:
            global WS_CLIENT
            if WS_CLIENT is None:
                WS_CLIENT = UMFuturesWS()
            # SDKs differ; support both signatures
            ok = False
            try:
                WS_CLIENT.kline(symbol=sym, interval=interval, callback=_ws_kline_cb)
                ok = True
            except Exception:
                try:
                    WS_CLIENT.kline(symbol=sym, id=f"{sym}-{interval}", interval=interval, callback=_ws_kline_cb)
                    ok = True
                except Exception as e:
                    techlog({"level":"warn","msg":"ws_kline_subscribe_failed","symbol":sym,"interval":interval,"err":str(e)})
            if ok:
                WS_STARTED.add(key)
                techlog({"level":"info","msg":"ws_kline_started","symbol":sym,"interval":interval,"import_path":_WS_IMPORT_PATH})
        except Exception as e:
            techlog({"level":"warn","msg":"ws_client_failed","err":str(e)})

# ===== SL-by-close helpers =====
def _last_closed_kline(symbol: str, interval: str):
    """First try WS cache; fallback to REST (rare)."""
    sym = symbol.upper()
    with WS_LOCK:
        if interval in KLINE_CACHE.get(sym, {}):
            return KLINE_CACHE[sym][interval]
    # REST fallback (rate-limited)
    try:
        kl = BINANCE.klines(symbol=sym, interval=interval, limit=3)
        if not kl or len(kl) < 2:
            techlog({"level":"warn","msg":"no_klines_rest","symbol":sym,"interval":interval,"len":(len(kl) if kl else 0)})
            return None
        k = kl[-2]
        return float(k[1]), float(k[2]), float(k[3]), float(k[4]), int(k[6])
    except Exception as e:
        techlog({"level":"warn","msg":"klines_rest_failed","symbol":sym,"interval":interval,"err":str(e)})
        return None

def _position_amt(symbol: str) -> float:
    try:
        data = _fetch_positions(symbol=symbol)
        amt = 0.0
        for p in data:
            if str(p.get("symbol","")).upper() == symbol.upper():
                try: amt = float(p.get("positionAmt") or p.get("positionamt") or 0.0)
                except Exception: amt = 0.0
                break
        return abs(amt)
    except Exception as e:
        techlog({"level":"warn","msg":"position_amt_failed","symbol":symbol,"err":str(e)})
        return 0.0

def _close_position_market(symbol: str, side: str):
    qty_pos = _position_amt(symbol)
    if qty_pos <= 0: return None
    exit_side = "SELL" if side == "long" else "BUY"
    filt = fetch_symbol_filters(symbol)
    step = (filt.get("stepSize") or 0.001)
    qty = q_floor_to_step(qty_pos, step)
    if qty <= 0: return None
    try:
        return BINANCE.new_order(symbol=symbol, side=exit_side, type="MARKET", quantity=qty, reduceOnly="true")
    except Exception as e:
        techlog({"level":"error","msg":"market_close_failed","symbol":symbol,"side":side,"err":str(e)})
        return None

def _fetch_trades_for_order(symbol: str, order_id: int):
    try:
        trades = BINANCE.user_trades(symbol=symbol)
    except Exception:
        try:
            trades = BINANCE.get_account_trades(symbol=symbol)
        except Exception as e:
            techlog({"level":"warn","msg":"user_trades_failed","symbol":symbol,"order_id":order_id,"err":str(e)})
            return None, None, None, "USDT", None
    flist = [t for t in trades if int(t.get("orderId", 0)) == int(order_id)]
    if not flist: return None, None, None, "USDT", None
    qty_sum = 0.0; px_qty_sum = 0.0; fee_sum = 0.0; pnl_sum = 0.0; fee_asset = "USDT"
    for t in flist:
        p = to_float(t.get("price")); q = to_float(t.get("qty"))
        fee = to_float(t.get("commission")) or 0.0
        fee_asset = t.get("commissionAsset") or fee_asset
        rpn = to_float(t.get("realizedPnl"))
        if p is not None and q is not None:
            qty_sum += q; px_qty_sum += p*q
        if rpn is not None: pnl_sum += rpn
        fee_sum += fee
    vwap = (px_qty_sum/qty_sum) if qty_sum else None
    return vwap, (qty_sum or None), (fee_sum or None), fee_asset, (pnl_sum if pnl_sum != 0.0 else None)

def _cancel_order_silent(symbol: str, order_id: int, reason: str):
    if not order_id: return
    try:
        BINANCE.cancel_order(symbol=symbol, orderId=order_id)
        techlog({"level":"info","msg":"cancel_order","symbol":symbol,"order_id":order_id,"reason":reason})
    except Exception as e:
        techlog({"level":"warn","msg":"cancel_order_failed","symbol":symbol,"order_id":order_id,"err":str(e)})

# ===== монітор =====
def _bracket_monitor():
    """Background: watch TP status; SL via closed-bar from WS cache."""
    while True:
        try:
            with BR_LOCK:
                items = list(BRACKETS.items())
            for symbol, b in items:
                sid   = b.get("id")
                side  = b.get("side")
                tp_id = b.get("tp_id")
                sl_v_raw  = b.get("sl_virtual_raw")
                interval = b.get("kline_interval", KLINE_INTERVAL)

                # guarantee ws stream running
                _start_ws_kline(symbol, interval)

                # cleanup if position = 0
                try:
                    pos_amt_now = _position_amt(symbol)
                except Exception:
                    pos_amt_now = 0.0
                if pos_amt_now == 0.0:
                    try:
                        BINANCE.cancel_all_open_orders(symbol=symbol)
                        techlog({"level":"info","msg":"cancel_all_on_zero_pos","symbol":symbol})
                    except Exception as e:
                        techlog({"level":"warn","msg":"cancel_all_failed","symbol":symbol,"err":str(e)})
                    with BR_LOCK:
                        if symbol in BRACKETS:
                            BRACKETS.pop(symbol, None)
                            techlog({"level":"info","msg":"bracket_removed_on_zero_position","symbol":symbol})
                    continue

                # 1) TP — біржовий
                if tp_id:
                    tp_st = _get_order_status(symbol, tp_id)
                    if tp_st == "FILLED":
                        vwap, qty, fee, fee_asset, rpn = _fetch_trades_for_order(symbol, tp_id)
                        iso = datetime.now(timezone.utc).isoformat().replace("+00:00","Z")
                        exec_log(sid, "CLOSE", iso, vwap, qty, fee, fee_asset, rpn, symbol, side, tp_id)
                        with BR_LOCK:
                            BRACKETS.pop(symbol, None)
                        continue

                # 2) SL — віртуальний по закриттю свічки
                if sl_v_raw is not None:
                    kl = _last_closed_kline(symbol, interval)
                    if kl:
                        _o,_h,_l,_c,_ct = kl
                        techlog({"level":"debug","msg":"sl_check","symbol":symbol,"side":side,"close":_c,"sl_raw":sl_v_raw,"interval":interval})
                        trigger = (side == "long" and _c <= float(sl_v_raw)) or (side == "short" and _c >= float(sl_v_raw))
                        if trigger:
                            oclose = _close_position_market(symbol, side)
                            if oclose:
                                oid = int(oclose.get("orderId", 0)) if isinstance(oclose, dict) else 0
                                vwap, qty, fee, fee_asset, rpn = _fetch_trades_for_order(symbol, oid)
                                iso = datetime.now(timezone.utc).isoformat().replace("+00:00","Z")
                                exec_log(sid, "CLOSE", iso, vwap, qty, fee, fee_asset, rpn, symbol, side, oid)
                            techlog({"level":"info","msg":"virtual_sl_close","symbol":symbol,"side":side,"close":_c,"sl_raw":sl_v_raw})
                            _cancel_order_silent(symbol, tp_id, reason="virtual_sl_triggered")
                            with BR_LOCK:
                                BRACKETS.pop(symbol, None)
                            continue
                    else:
                        techlog({"level":"debug","msg":"sl_skip_no_kline_cache","symbol":symbol,"interval":interval})

        except Exception as e:
            techlog({"level":"warn","msg":"bracket_monitor_error","err":str(e)})
        time.sleep(BRACKET_POLL_SEC)

def _after_open_arming_check(symbol: str):
    time.sleep(2)
    with BR_LOCK:
        armed = symbol in BRACKETS
    if armed:
        techlog({"level":"info","msg":"bracket_armed","symbol":symbol})
    else:
        techlog({"level":"warn","msg":"bracket_missing_after_open","symbol":symbol})

def _load_last_sl_from_csv(symbol_binance: str):
    tv1 = symbol_binance
    tv2 = symbol_binance + ".P"
    if not os.path.exists(CSV_PATH): return None
    try:
        last_sl = None
        with open(CSV_PATH, "r", encoding="utf-8") as f:
            r = csv.DictReader(f)
            for row in r:
                sym = (row.get("symbol") or "").upper()
                if sym in (tv1, tv2):
                    try: last_sl = float(row.get("sl"))
                    except Exception: pass
        return last_sl
    except Exception as e:
        techlog({"level":"warn","msg":"load_csv_sl_failed","err":str(e)})
        return None

def _recover_state():
    if not PRESET_SYMBOLS: return
    for s in PRESET_SYMBOLS:
        try:
            if _position_amt(s) > 0 and s not in BRACKETS:
                # знайти TP
                tp_id = None
                orders = _list_open_orders(s) or []
                for od in orders:
                    if str(od.get("type","")).upper() in ("TAKE_PROFIT_MARKET","TAKE_PROFIT") and str(od.get("closePosition","")).lower() in ("true","1","yes"):
                        tp_id = int(od.get("orderId", 0)); break
                sl_raw = _load_last_sl_from_csv(s)
                with BR_LOCK:
                    BRACKETS[s] = {
                        "id": f"recover|{s}|{int(time.time())}",
                        "side": "long" if any(float(p.get("positionAmt",0))>0 for p in _fetch_positions(s)) else "short",
                        "tp_id": tp_id,
                        "sl_id": None,
                        "sl_virtual_raw": sl_raw,
                        "kline_interval": KLINE_INTERVAL,
                        "open_order_id": 0,
                        "ts": datetime.now(timezone.utc).isoformat().replace("+00:00","Z")
                    }
                _start_ws_kline(s, KLINE_INTERVAL)
                techlog({"level":"info","msg":"state_recovered","symbol":s,"tp_id":tp_id,"sl_raw":sl_raw})
        except Exception as e:
            techlog({"level":"warn","msg":"state_recover_failed","symbol":s,"err":str(e)})

def place_orders_oneway(symbol: str, side: str, entry: float, tp: float, sl: float, signal_id: str):
    symbol = symbol.upper()
    ensure_oneway_mode()
    ensure_leverage(symbol)

    with BR_LOCK:
        has_bracket = symbol in BRACKETS
    if has_bracket:
        if not REPLACE_ON_NEW:
            techlog({"level":"info","msg":"skip_new_order_active_bracket","symbol":symbol})
            return {"skipped": True, "reason": "active_bracket"}
        old = None
        with BR_LOCK:
            old = BRACKETS.get(symbol, {})
        _cancel_order_silent(symbol, old.get("tp_id"), reason="replace_on_new")
        _close_position_market(symbol, old.get("side", side))
        with BR_LOCK:
            BRACKETS.pop(symbol, None)
        techlog({"level":"info","msg":"replaced_old_bracket","symbol":symbol})

    pos_amt = _position_amt(symbol)
    if pos_amt > 0.0 and REPLACE_ON_NEW:
        _close_position_market(symbol, side)
        techlog({"level":"info","msg":"closed_existing_pos_before_new","symbol":symbol,"pos_amt":pos_amt})
    elif pos_amt > 0.0 and not REPLACE_ON_NEW:
        techlog({"level":"info","msg":"skip_new_order_existing_pos","symbol":symbol,"pos_amt":pos_amt})
        return {"skipped": True, "reason": "existing_position"}

    price = get_mark_price(symbol)
    qty   = compute_qty(symbol, price)

    filt = fetch_symbol_filters(symbol); tick = filt["tickSize"] or 0.0001
    tp_r = p_floor_to_tick(tp, tick)
    sl_raw = float(sl)

    open_side = "BUY" if side == "long" else "SELL"
    o1 = BINANCE.new_order(symbol=symbol, side=open_side, type="MARKET", quantity=qty)
    open_id = int(o1.get("orderId"))
    techlog({"level":"info","msg":"open_order_ok","symbol":symbol,"side":side,"qty":qty,"order_id":open_id})

    for _ in range(8):
        st = _get_order_status(symbol, open_id)
        if st == "FILLED": break
        time.sleep(0.3)
    vwap_open, qty_open, fee_open, fee_asset_open, _ = _fetch_trades_for_order(symbol, open_id)
    iso_open = datetime.now(timezone.utc).isoformat().replace("+00:00","Z")
    exec_log(signal_id, "OPEN", iso_open, vwap_open, qty_open, fee_open, fee_asset_open, None, symbol, side, open_id)

    with BR_LOCK:
        BRACKETS[symbol] = {
            "id": signal_id,
            "side": side,
            "tp_id": None,
            "sl_id": None,
            "sl_virtual_raw": sl_raw,
            "kline_interval": KLINE_INTERVAL,
            "open_order_id": open_id,
            "ts": datetime.now(timezone.utc).isoformat().replace("+00:00","Z")
        }
    techlog({"level":"info","msg":"bracket_seeded","symbol":symbol,"sl_raw":sl_raw})
    threading.Thread(target=_after_open_arming_check, args=(symbol,), daemon=True).start()

    # запустити ws-стрім для цього символу (щоб SL працював відразу)
    _start_ws_kline(symbol, KLINE_INTERVAL)

    exit_side = "SELL" if side == "long" else "BUY"
    try:
        oTP = BINANCE.new_order(
            symbol=symbol, side=exit_side, type="TAKE_PROFIT_MARKET",
            stopPrice=tp_r, closePosition="true", workingType="MARK_PRICE"
        )
        tp_id = int(oTP.get("orderId"))
        with BR_LOCK:
            if symbol in BRACKETS:
                BRACKETS[symbol]["tp_id"] = tp_id
        techlog({"level":"info","msg":"tp_order_ok","symbol":symbol,"tp":tp_r,"tp_id":tp_id})
    except Exception as e:
        techlog({"level":"warn","msg":"tp_order_failed","symbol":symbol,"tp":tp_r,"err":str(e)})

    print(f"[TRADE] {symbol} {side} qty={qty} price={price} tp={tp_r} sl_virtual_raw={sl_raw}", flush=True)
    techlog({"level":"info","msg":"binance_orders_placed",
             "symbol":symbol,"qty":qty,"open_side":open_side,"tp":tp_r,"sl_virtual_raw":sl_raw,
             "tp_id":(BRACKETS.get(symbol,{}).get('tp_id') if symbol in BRACKETS else None),
             "sl_id":None,"open_order_id":open_id,"price":price,"mode":"oneway"})
    return {"qty":qty, "price":price, "tp":tp_r, "sl_virtual_raw":sl_raw, "open_order":o1}

# ===== Binance init =====
BINANCE = None
if BINANCE_ENABLED and UMFutures:
    try:
        if TESTNET:
            BINANCE = UMFutures(key=API_KEY_TEST, secret=API_SECRET_TEST, base_url="https://testnet.binancefuture.com")
        else:
            BINANCE = UMFutures(key=API_KEY_MAIN, secret=API_SECRET_MAIN)
        techlog({"level":"info","msg":"binance_client_ready","import_path":_BINANCE_IMPORT_PATH,"testnet":TESTNET})
        if PRESET_SYMBOLS:
            ensure_oneway_mode()
            for s in PRESET_SYMBOLS:
                try:
                    BINANCE.change_leverage(symbol=s, leverage=LEVERAGE)
                    LEVERAGE_SET.add(s)
                    techlog({"level":"info","msg":"preset_leverage_ok","symbol":s,"leverage":LEVERAGE})
                except Exception as e:
                    techlog({"level":"warn","msg":"preset_leverage_failed","symbol":s,"err":str(e)})
        threading.Thread(target=_bracket_monitor, daemon=True).start()
        techlog({"level":"info","msg":"bracket_monitor_started","poll_sec":BRACKET_POLL_SEC,"kline_interval":KLINE_INTERVAL})
        # старт WS для пресетів одразу — щоб кеш наповнювався
        for s in PRESET_SYMBOLS:
            _start_ws_kline(s, KLINE_INTERVAL)
        _recover_state()
    except Exception as e:
        techlog({"level":"warn","msg":"binance_client_init_failed","err":str(e)})
        BINANCE_ENABLED = False
        BINANCE = None

# ===== routes =====
@app.route("/", methods=["GET"])
def root():
    return "Bot is live", 200

@app.route("/healthz", methods=["GET"])
def healthz():
    return jsonify({
        "status":"ok",
        "version":"2.7.0+ws_kline_monitor",
        "env":ENV_MODE,
        "trading_enabled": BINANCE_ENABLED,
        "testnet": TESTNET,
        "risk_mode": RISK_MODE,
        "risk_pct": RISK_PCT,
        "leverage": LEVERAGE,
        "preset_symbols": PRESET_SYMBOLS,
        "binance_import_path": _BINANCE_IMPORT_PATH,
        "ws_import_path": _WS_IMPORT_PATH,
        "cancel_orphans": CANCEL_ORPHANS,
        "poll_sec": BRACKET_POLL_SEC,
        "kline_interval": KLINE_INTERVAL,
        "replace_on_new": REPLACE_ON_NEW,
        "time": datetime.now(timezone.utc).isoformat().replace("+00:00","Z")
    }), 200

@app.route("/config", methods=["GET","POST"])
def config():
    global RISK_MODE, RISK_PCT, LEVERAGE
    if request.method == "GET":
        return jsonify({
            "risk_mode": RISK_MODE, "risk_pct": RISK_PCT, "leverage": LEVERAGE,
            "allow_pattern": ALLOW_PATTERN, "trading_enabled": BINANCE_ENABLED, "testnet": TESTNET,
            "replace_on_new": REPLACE_ON_NEW, "kline_interval": KLINE_INTERVAL
        }), 200
    if ADMIN_TOKEN and request.headers.get("X-Admin-Token","") != ADMIN_TOKEN:
        return jsonify({"status":"error","msg":"unauthorized"}), 401
    try:
        data = request.get_json(force=True, silent=False)
    except Exception:
        return jsonify({"status":"error","msg":"bad json"}), 400
    if "risk_mode" in data:
        val = str(data["risk_mode"]).lower()
        if val not in ("margin","notional"):
            return jsonify({"status":"error","msg":"risk_mode must be margin|notional"}), 400
        RISK_MODE = val
    if "risk_pct" in data:
        try:
            v = float(data["risk_pct"])
            if not (0 < v <= 100):
                return jsonify({"status":"error","msg":"risk_pct must be (0;100]"}), 400
            RISK_PCT = v
        except Exception:
            return jsonify({"status":"error","msg":"risk_pct must be float"}), 400
    if "leverage" in data:
        try:
            lv = int(data["leverage"])
            if lv < 1 or lv > 125:
                return jsonify({"status":"error","msg":"leverage out of range"}), 400
            LEVERAGE = lv
            LEVERAGE_SET.clear()
            techlog({"level":"info","msg":"leverage_config_changed","leverage":LEVERAGE})
        except Exception:
            return jsonify({"status":"error","msg":"leverage must be int"}), 400
    techlog({"level":"info","msg":"config_updated","risk_mode":RISK_MODE,"risk_pct":RISK_PCT,"leverage":LEVERAGE})
    return jsonify({"status":"ok","risk_mode":RISK_MODE,"risk_pct":RISK_PCT,"leverage":LEVERAGE}), 200

@app.route("/leverage", methods=["POST"])
def set_leverage_now():
    if ADMIN_TOKEN and request.headers.get("X-Admin-Token","") != ADMIN_TOKEN:
        return jsonify({"status":"error","msg":"unauthorized"}), 401
    if not BINANCE_ENABLED or not BINANCE:
        return jsonify({"status":"error","msg":"trading_disabled"}), 400
    try:
        data = request.get_json(force=True, silent=False)
    except Exception:
        return jsonify({"status":"error","msg":"bad json"}), 400
    symbols = data.get("symbols")
    leverage = int(data.get("leverage", LEVERAGE))
    if not symbols or not isinstance(symbols, list):
        return jsonify({"status":"error","msg":"symbols must be list"}), 400
    ensure_oneway_mode()
    done, failed = [], []
    for sym in [str(s).upper() for s in symbols]:
        try:
            BINANCE.change_leverage(symbol=sym, leverage=leverage)
            LEVERAGE_SET.add(sym); done.append(sym)
        except Exception as e:
            failed.append({"symbol": sym, "err": str(e)})
    techlog({"level":"info","msg":"manual_leverage_set","done":done,"failed":failed,"leverage":leverage})
    return jsonify({"status":"ok","done":done,"failed":failed,"leverage":leverage}), 200

# ---- Webhook ----
@app.route("/webhook", methods=["POST"])
def webhook():
    print(f"[WEBHOOK_RX] headers={dict(request.headers)}", flush=True)
    if not valid_sig(request):
        techlog({"level":"warn","msg":"bad_signature","headers":dict(request.headers)})
        return jsonify({"status":"error","msg":"bad signature"}), 401
    try:
        raw = request.get_data(cache=False, as_text=True)
        data = json.loads(raw)
    except Exception as e:
        techlog({"level":"error","msg":"bad_json","err":str(e)})
        return jsonify({"status":"error","msg":f"bad json: {e}"}), 400

    ok, info = validate_payload(data)
    if not ok:
        techlog({"level":"warn","msg":"bad_payload","detail":info,"data":data})
        return jsonify({"status":"error","msg":info}), 400

    symbol_tv  = str(data["symbol"])
    time_s     = str(data["time"])
    side       = str(data["side"]).lower()
    pattern    = str(data["pattern"]).lower()
    entry_val  = float(info["entry"])
    sig_id     = build_id(pattern, side, time_s, entry_val)

    print(f"[WEBHOOK_OK] id={sig_id} data={json.dumps(data, ensure_ascii=False)}", flush=True)

    symbol = tv_to_binance_symbol(symbol_tv)

    with BR_LOCK:
        has_local_bracket = symbol in BRACKETS
    if BINANCE_ENABLED and UMFutures and BINANCE and has_local_bracket:
        try:
            if _position_amt(symbol) == 0.0:
                oo = _list_open_orders(symbol) or []
                if not oo:
                    with BR_LOCK:
                        BRACKETS.pop(symbol, None)
                    techlog({"level":"info","msg":"stale_bracket_purged","symbol":symbol,"id":sig_id})
                    has_local_bracket = False
        except Exception as e:
            techlog({"level":"warn","msg":"stale_purge_check_failed","symbol":symbol,"err":str(e)})

    has_exchange_pos  = (_position_amt(symbol) > 0.0) if (BINANCE_ENABLED and UMFutures and BINANCE) else False

    if (has_local_bracket or has_exchange_pos) and not REPLACE_ON_NEW:
        techlog({"level":"info","msg":"ignored_new_signal_active_position","id":sig_id,"symbol":symbol})
        return jsonify({"status":"ok","msg":"ignored_active_position","id":sig_id}), 200

    if dedup_seen(sig_id):
        techlog({"level":"info","msg":"duplicate_ignored","id":sig_id})
        return jsonify({"status":"ok","msg":"ignored","id":sig_id}), 200

    rotate_if_needed(CSV_PATH)

    time_iso = to_iso8601(time_s)
    row = [time_s, time_iso, symbol_tv, pattern, side, info["entry"], info["tp"], info["sl"], sig_id]
    newfile = not os.path.exists(CSV_PATH)
    try:
        with open(CSV_PATH, "a", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            if newfile:
                w.writerow(["time_raw","time_iso","symbol","pattern","side","entry","tp","sl","id"])
            w.writerow(row)
    except Exception as e:
        techlog({"level":"error","msg":"csv_write_failed","err":str(e),"row":row})
        return jsonify({"status":"error","msg":"csv write failed"}), 500

    techlog({"level":"info","msg":"logged","id":sig_id,"symbol":symbol_tv,"side":side})

    if BINANCE_ENABLED and BINANCE:
        try:
            place_orders_oneway(symbol=symbol, side=side, entry=info["entry"], tp=info["tp"], sl=info["sl"], signal_id=sig_id)
            techlog({"level":"info","msg":"trade_ok","id":sig_id,"symbol":symbol})
        except Exception as e:
            techlog({"level":"error","msg":"trade_failed","id":sig_id,"err":str(e)})
    else:
        techlog({"level":"info","msg":"trading_disabled","id":sig_id})

    return jsonify({"status":"ok","msg":"logged","id":sig_id}), 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", "5000")))
