# -*- coding: utf-8 -*-
import os, json, csv, hmac, hashlib, threading, time, re
from datetime import datetime, timezone
from decimal import Decimal, ROUND_DOWN
from collections import OrderedDict
from flask import Flask, request, jsonify

# ====== CONFIG FROM ENV ======
BINANCE_ENABLED = os.environ.get("TRADING_ENABLED", "false").lower() == "true"

TESTNET  = os.environ.get("TESTNET", "false").lower() == "true"
API_KEY_MAIN    = os.environ.get("BINANCE_API_KEY", "")
API_SECRET_MAIN = os.environ.get("BINANCE_API_SECRET", "")
API_KEY_TEST    = os.environ.get("BINANCE_API_KEY_TEST", "")
API_SECRET_TEST = os.environ.get("BINANCE_API_SECRET_TEST", "")

LEVERAGE   = int(os.environ.get("LEVERAGE", "10"))
RISK_MODE  = os.environ.get("RISK_MODE", "margin").lower()     # margin | notional
RISK_PCT   = float(os.environ.get("RISK_PCT", "1.0"))

ALLOW_PATTERN = os.environ.get("ALLOW_PATTERN", "engulfing").lower()
REPLACE_ON_NEW = os.environ.get("REPLACE_ON_NEW", "false").lower() == "true"
PRESET_SYMBOLS = [x.strip().upper() for x in os.environ.get("PRESET_SYMBOLS","").split(",") if x.strip()]

SECRET      = os.environ.get("WEBHOOK_SECRET", "")
ADMIN_TOKEN = os.environ.get("ADMIN_TOKEN", "")

LOG_DIR   = os.environ.get("LOG_DIR", "logs")
LOG_FILE  = os.environ.get("LOG_FILE", "engulfing.csv")
TECH_LOG  = os.environ.get("TECH_LOG", "tech.jsonl")
EXEC_LOG  = os.environ.get("EXEC_LOG", "executions.csv")
ROTATE_BYTES = int(os.environ.get("ROTATE_BYTES", str(5*1024*1024)))
KEEP_FILES   = int(os.environ.get("KEEP_FILES", "3"))
MAX_KEYS     = int(os.environ.get("DEDUP_CACHE", "2000"))

# NEW: контрольні інтервали та прибирання сиріт
BRACKET_POLL_SEC = float(os.environ.get("BRACKET_POLL_SEC", "2"))
CANCEL_ORPHANS   = os.environ.get("CANCEL_ORPHANS", "true").lower() == "true"
ORPHAN_SWEEP_SEC = float(os.environ.get("ORPHAN_SWEEP_SEC", "10"))
CANCEL_RETRIES   = int(os.environ.get("CANCEL_RETRIES", "3"))

os.makedirs(LOG_DIR, exist_ok=True)
CSV_PATH  = os.path.join(LOG_DIR, LOG_FILE)
TECH_PATH = os.path.join(LOG_DIR, TECH_LOG)
EXEC_PATH = os.path.join(LOG_DIR, EXEC_LOG)

# ====== BINANCE CLIENT ======
UMFutures = None
_BINANCE_IMPORT_PATH = None
if BINANCE_ENABLED:
    try:
        from binance.um_futures import UMFutures as _UM
        UMFutures = _UM
        _BINANCE_IMPORT_PATH = "binance.um_futures"
    except Exception:
        try:
            from binance.lib.um_futures import UMFutures as _UM
            UMFutures = _UM
            _BINANCE_IMPORT_PATH = "binance.lib.um_futures"
        except Exception as e:
            print("[WARN] Cannot import UMFutures:", repr(e), flush=True)
            BINANCE_ENABLED = False

BINANCE = None
ONEWAY_SET = False
LEVERAGE_SET = set()
SYMBOL_CACHE = {}

# ====== APP ======
app = Flask(__name__)

# ====== STATE ======
DEDUP = OrderedDict()
# BRACKETS[symbol] = { id, side, tp_id, sl_id, open_order_id, ts }
BRACKETS = {}
BR_LOCK = threading.RLock()

# ====== UTIL ======
def techlog(entry: dict):
    entry["ts"] = datetime.now(timezone.utc).isoformat().replace("+00:00","Z")
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
                if i == KEEP_FILES: os.remove(src)
                else: os.rename(src, dst)
        os.rename(path, f"{path}.1")
    except Exception as e:
        techlog({"level":"warn","msg":"rotate_failed","err":str(e),"path":path})

def _ensure_exec_header():
    if not os.path.exists(EXEC_PATH) or os.path.getsize(EXEC_PATH) == 0:
        with open(EXEC_PATH, "w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow(
                ["signal_id","event","time","price","qty","commission","commission_asset","realized_pnl","symbol","side","order_id"]
            )

def exec_log(signal_id, event, iso_time, price, qty, commission, commission_asset, realized_pnl, symbol, side, order_id):
    rotate_if_needed(EXEC_PATH); _ensure_exec_header()
    with open(EXEC_PATH, "a", newline="", encoding="utf-8") as f:
        csv.writer(f).writerow([signal_id,event,iso_time,price or "",qty or "",commission or "",commission_asset or "",
                                realized_pnl if realized_pnl is not None else "",symbol,side,order_id])

def to_float(x):
    try: return float(x)
    except: return None

def to_iso8601(ts):
    s=str(ts)
    try:
        v=float(s); iv=int(v)
        if iv>10_000_000_000: return datetime.fromtimestamp(iv/1000, tz=timezone.utc).isoformat().replace("+00:00","Z")
        if iv>1_000_000_000:  return datetime.fromtimestamp(iv, tz=timezone.utc).isoformat().replace("+00:00","Z")
    except: pass
    try:
        if s.endswith("Z"): s=s[:-1]+"+00:00"
        return datetime.fromisoformat(s).astimezone(timezone.utc).isoformat().replace("+00:00","Z")
    except: return datetime.now(timezone.utc).isoformat().replace("+00:00","Z")

def calc_sig(raw): 
    return hmac.new(SECRET.encode(), raw, hashlib.sha256).hexdigest() if SECRET else ""

def valid_sig(req):
    if not SECRET: return True
    try:
        return hmac.compare_digest(req.headers.get("X-Signature",""), calc_sig(req.get_data(cache=False, as_text=False)))
    except: return False

def tv_to_binance_symbol(tv_symbol: str) -> str:
    return re.sub(r"\.P$", "", str(tv_symbol)).upper()

def q_floor_to_step(qty: float, step: float) -> float:
    if step<=0: return qty
    from decimal import Decimal as D
    return float((D(str(qty))/D(str(step))).to_integral_value(rounding=ROUND_DOWN)*D(str(step)))

def p_floor_to_tick(price: float, tick: float) -> float:
    if tick<=0: return price
    from decimal import Decimal as D
    m=D(str(price))/D(str(tick))
    return float(m.to_integral_value(rounding=ROUND_DOWN)*D(str(tick)))

def fetch_symbol_filters(symbol: str):
    symbol=symbol.upper()
    if symbol in SYMBOL_CACHE: return SYMBOL_CACHE[symbol]
    info = BINANCE.exchange_info()
    filt={"stepSize":None,"tickSize":None,"minQty":None,"minNotional":None}
    for s in info.get("symbols",[]):
        if s.get("symbol")==symbol:
            for f in s.get("filters",[]):
                t=f.get("filterType")
                if t=="LOT_SIZE": filt["stepSize"]=float(f["stepSize"]); filt["minQty"]=float(f["minQty"])
                elif t=="PRICE_FILTER": filt["tickSize"]=float(f["tickSize"])
                elif t in ("MIN_NOTIONAL","NOTIONAL"):
                    mn=f.get("notional") or f.get("minNotional")
                    if mn is not None: filt["minNotional"]=float(mn)
            SYMBOL_CACHE[symbol]=filt; return filt
    raise ValueError(f"Symbol {symbol} not found")

def get_available_balance_usdt():
    for b in BINANCE.balance():
        if b.get("asset")=="USDT":
            return float(b.get("availableBalance"))
    return 0.0

def get_mark_price(symbol): 
    return float(BINANCE.mark_price(symbol=symbol)["markPrice"])

def ensure_oneway_mode():
    global ONEWAY_SET
    if ONEWAY_SET: return
    try:
        try:
            dual = str(BINANCE.get_position_mode().get("dualSidePosition","false")).lower() in ("true","1")
            if not dual: ONEWAY_SET=True; techlog({"level":"info","msg":"oneway_mode_ok","source":"precheck"}); return
        except: pass
        BINANCE.change_position_mode(dualSidePosition="false")
        ONEWAY_SET=True; techlog({"level":"info","msg":"oneway_mode_ok"})
    except Exception as e:
        techlog({"level":"warn","msg":"change_position_mode_failed","err":str(e)})

def ensure_leverage(symbol):
    if symbol in LEVERAGE_SET: return
    try:
        BINANCE.change_leverage(symbol=symbol, leverage=LEVERAGE)
        LEVERAGE_SET.add(symbol)
        techlog({"level":"info","msg":"leverage_ok","symbol":symbol,"leverage":LEVERAGE})
    except Exception as e:
        techlog({"level":"warn","msg":"change_leverage_failed","symbol":symbol,"err":str(e)})

def _fetch_positions(symbol):
    methods=["position_risk","get_position_risk","position_information","get_position_information"]
    for m in methods:
        if hasattr(BINANCE, m):
            try:
                data=getattr(BINANCE,m)(symbol=symbol)
                if isinstance(data,dict) and "positions" in data: return data["positions"]
                if isinstance(data,list): return data
            except: pass
    return []

def _position_amt(symbol)->float:
    try:
        for p in _fetch_positions(symbol):
            if str(p.get("symbol","")).upper()==symbol.upper():
                return abs(float(p.get("positionAmt") or 0.0))
    except: pass
    return 0.0

def _get_order_status(symbol, order_id:int) -> str:
    try: od = BINANCE.get_order(symbol=symbol, orderId=order_id)
    except: od = BINANCE.query_order(symbol=symbol, orderId=order_id)
    return str(od.get("status","")).upper()

def _list_open_orders(symbol=None):
    """
    Повертає відкриті ордери:
      - якщо бібліотека дозволяє, без symbol → усі,
      - інакше для конкретного symbol.
    """
    for m in ("get_open_orders","open_orders"):
        if hasattr(BINANCE, m):
            fn = getattr(BINANCE, m)
            try:
                if symbol is None:
                    return fn()
                else:
                    return fn(symbol=symbol)
            except:
                if symbol is not None:
                    continue
    return []

def _fetch_trades_for_order(symbol, order_id:int):
    try: trades = BINANCE.user_trades(symbol=symbol)
    except:
        try: trades = BINANCE.get_account_trades(symbol=symbol)
        except Exception as e:
            techlog({"level":"warn","msg":"user_trades_failed","symbol":symbol,"order_id":order_id,"err":str(e)})
            return None,None,None,"USDT",None
    fl=[t for t in trades if int(t.get("orderId",0))==int(order_id)]
    if not fl: return None,None,None,"USDT",None
    qty=0.0; pxqty=0.0; fee=0.0; pnl=0.0; asset="USDT"
    for t in fl:
        p=to_float(t.get("price")); q=to_float(t.get("qty")); f=to_float(t.get("commission")) or 0.0
        asset=t.get("commissionAsset") or asset; rpn=to_float(t.get("realizedPnl"))
        if p is not None and q is not None: qty+=q; pxqty+=p*q
        if rpn is not None: pnl+=rpn
        fee+=f
    vwap=(pxqty/qty) if qty else None
    return vwap,(qty or None),(fee or None),asset,(pnl if pnl!=0.0 else None)

# ====== HEARTBEAT ======
def _heartbeat():
    while True:
        print(f"[HEARTBEAT] {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} alive", flush=True)
        time.sleep(60)
threading.Thread(target=_heartbeat, daemon=True).start()

# ====== CANCEL HELPERS ======
def _cancel_order_silent(symbol, order_id, reason):
    if not order_id: return
    err = None
    for i in range(max(1, CANCEL_RETRIES)):
        try:
            BINANCE.cancel_order(symbol=symbol, orderId=order_id)
            techlog({"level":"info","msg":"cancel_order","symbol":symbol,"order_id":order_id,"reason":reason,"try":i+1})
            return
        except Exception as e:
            err = str(e)
            time.sleep(0.3)
    techlog({"level":"warn","msg":"cancel_order_failed","symbol":symbol,"order_id":order_id,"err":err})

def _cancel_all_silent(symbol, reason):
    err = None
    for i in range(max(1, CANCEL_RETRIES)):
        try:
            BINANCE.cancel_all_open_orders(symbol=symbol)
            techlog({"level":"info","msg":"cancel_all_open_orders","symbol":symbol,"reason":reason,"try":i+1})
            return
        except Exception as e:
            err = str(e)
            time.sleep(0.3)
    techlog({"level":"warn","msg":"cancel_all_failed","symbol":symbol,"err":err,"reason":reason})

# ====== BRACKET MONITOR (only exchange TP/SL) ======
def _bracket_monitor():
    while True:
        try:
            with BR_LOCK:
                items=list(BRACKETS.items())
            for symbol, b in items:
                sid=b.get("id"); side=b.get("side")
                tp_id=b.get("tp_id"); sl_id=b.get("sl_id")

                # Якщо позиції немає — прибрати локальну пам'ять, скасувати залишки
                if _position_amt(symbol)==0.0:
                    _cancel_all_silent(symbol, "pos_is_zero")
                    with BR_LOCK: BRACKETS.pop(symbol, None)
                    techlog({"level":"info","msg":"bracket_removed_on_zero_position","symbol":symbol})
                    continue

                # Перевірка TP/SL статусів
                if tp_id:
                    st=_get_order_status(symbol, tp_id)
                    if st=="FILLED":
                        vwap,qty,fee,asset,rpn=_fetch_trades_for_order(symbol, tp_id)
                        exec_log(sid,"CLOSE",datetime.now(timezone.utc).isoformat().replace("+00:00","Z"),
                                 vwap,qty,fee,asset,rpn,symbol,side,tp_id)
                        _cancel_order_silent(symbol, sl_id, "tp_filled")
                        with BR_LOCK: BRACKETS.pop(symbol,None)
                        continue
                if sl_id:
                    st=_get_order_status(symbol, sl_id)
                    if st=="FILLED":
                        vwap,qty,fee,asset,rpn=_fetch_trades_for_order(symbol, sl_id)
                        exec_log(sid,"CLOSE",datetime.now(timezone.utc).isoformat().replace("+00:00","Z"),
                                 vwap,qty,fee,asset,rpn,symbol,side,sl_id)
                        _cancel_order_silent(symbol, tp_id, "sl_filled")
                        with BR_LOCK: BRACKETS.pop(symbol,None)
                        continue
        except Exception as e:
            techlog({"level":"warn","msg":"bracket_monitor_error","err":str(e)})
        time.sleep(max(0.5, BRACKET_POLL_SEC))

# ====== ORPHAN SWEEPER & RECOVERY ======
def _recover_state():
    """Відновлюємо BRACKETS для відкритих позицій і чистимо сиріт без позиції."""
    # список символів, які точно варто перевірити
    candidates = set([s for s in PRESET_SYMBOLS if s])  # із ENV
    # + усі символи з відкритими ордерами, якщо SDK дозволяє
    try:
        all_open = _list_open_orders(symbol=None) or []
        for od in all_open:
            if "symbol" in od:
                candidates.add(str(od["symbol"]).upper())
    except:  # у разі невдачі просто працюємо з PRESET_SYMBOLS
        pass

    for s in sorted(candidates):
        try:
            amt = _position_amt(s)
            orders = _list_open_orders(s) or []
            # якщо позиції немає, але є closePosition ордери — приберемо
            if amt == 0.0:
                has_close = any(str(od.get("closePosition","")).lower() in ("true","1","yes") for od in orders)
                if has_close and CANCEL_ORPHANS:
                    _cancel_all_silent(s, "recover_cleanup_orphans")
                    techlog({"level":"info","msg":"orphans_cleaned","symbol":s})
                continue

            # якщо позиція є — відновимо локальний BRACKET
            tp_id = sl_id = None
            for od in orders:
                typ = str(od.get("type","")).upper()
                if str(od.get("closePosition","")).lower() not in ("true","1","yes"):
                    continue
                if typ in ("TAKE_PROFIT_MARKET","TAKE_PROFIT"):
                    tp_id = int(od.get("orderId",0))
                elif typ in ("STOP_MARKET","STOP"):
                    sl_id = int(od.get("orderId",0))

            side = "long" if any(float(p.get("positionAmt",0))>0 for p in _fetch_positions(s)) else "short"
            with BR_LOCK:
                BRACKETS[s] = {
                    "id": f"recover|{s}|{int(time.time())}",
                    "side": side,
                    "tp_id": tp_id,
                    "sl_id": sl_id,
                    "open_order_id": 0,
                    "ts": datetime.now(timezone.utc).isoformat().replace("+00:00","Z")
                }
            techlog({"level":"info","msg":"state_recovered","symbol":s,"tp_id":tp_id,"sl_id":sl_id})
        except Exception as e:
            techlog({"level":"warn","msg":"state_recover_failed","symbol":s,"err":str(e)})

def _orphan_sweeper():
    """Періодично чистимо open closePosition ордери, якщо позиції нема (навіть без BRACKETS)."""
    if not CANCEL_ORPHANS:
        return
    while True:
        try:
            # пробуємо зібрати весь список ордерів (або проходимо PRESET_SYMBOLS)
            symbols = set()
            try:
                all_open = _list_open_orders(symbol=None) or []
                for od in all_open:
                    sy = str(od.get("symbol","")).upper()
                    if sy: symbols.add(sy)
            except:
                symbols.update([s for s in PRESET_SYMBOLS if s])

            for s in sorted(symbols):
                try:
                    if _position_amt(s) == 0.0:
                        # залишились будь-які open orders? приберемо
                        if _list_open_orders(s):
                            _cancel_all_silent(s, "orphan_sweeper")
                except Exception as e:
                    techlog({"level":"warn","msg":"orphan_sweep_symbol_failed","symbol":s,"err":str(e)})
        except Exception as e:
            techlog({"level":"warn","msg":"orphan_sweeper_failed","err":str(e)})
        time.sleep(max(3.0, ORPHAN_SWEEP_SEC))

# ====== RISK/QTY ======
def compute_qty(symbol, price):
    bal = get_available_balance_usdt()
    if RISK_MODE=="margin":
        notional = bal*(RISK_PCT/100.0)*LEVERAGE
    else:
        notional = bal*(RISK_PCT/100.0)
    qty_raw = notional/max(price, 1e-12)

    f=fetch_symbol_filters(symbol)
    step=f.get("stepSize") or 0.001
    min_qty=f.get("minQty") or 0.0
    min_not=f.get("minNotional") or 0.0

    qty=q_floor_to_step(qty_raw, step)
    if min_qty and qty<min_qty: qty=min_qty
    if min_not and (qty*price)<min_not:
        qty=q_floor_to_step((min_not/price), step) or min_qty
    if qty<=0: raise RuntimeError("Computed qty <= 0")
    return qty

# ====== ENTRY/BRACKET (exchange TP/SL only) ======
def place_orders_oneway(symbol: str, side: str, entry: float, tp: float, sl: float, signal_id: str):
    symbol=symbol.upper()
    ensure_oneway_mode(); ensure_leverage(symbol)

    # single-position rules
    with BR_LOCK: has_local = symbol in BRACKETS
    if has_local and not REPLACE_ON_NEW:
        techlog({"level":"info","msg":"skip_new_order_active_bracket","symbol":symbol})
        return {"skipped":True,"reason":"active_bracket"}

    pos_amt=_position_amt(symbol)
    if (has_local or pos_amt>0.0) and REPLACE_ON_NEW:
        try: _cancel_all_silent(symbol, "replace_on_new")
        except: pass
        # закриваємо ринком протилежним side
        exit_side="SELL" if side=="long" else "BUY"
        try:
            filt=fetch_symbol_filters(symbol); step=filt.get("stepSize") or 0.001
            BINANCE.new_order(symbol=symbol, side=exit_side, type="MARKET", reduceOnly="true",
                              quantity=q_floor_to_step(pos_amt, step))
        except Exception as e:
            techlog({"level":"warn","msg":"replace_market_close_failed","symbol":symbol,"err":str(e)})
        with BR_LOCK: BRACKETS.pop(symbol, None)
        techlog({"level":"info","msg":"replaced_old_position","symbol":symbol})

    if pos_amt>0.0 and not REPLACE_ON_NEW:
        techlog({"level":"info","msg":"skip_new_order_existing_pos","symbol":symbol,"pos_amt":pos_amt})
        return {"skipped":True,"reason":"existing_position"}

    price=get_mark_price(symbol)
    qty  =compute_qty(symbol, price)
    filt =fetch_symbol_filters(symbol)
    tick =filt.get("tickSize") or 0.0001

    tp_r=p_floor_to_tick(float(tp), tick)
    sl_r=p_floor_to_tick(float(sl), tick)

    # 1) OPEN (MARKET)
    open_side="BUY" if side=="long" else "SELL"
    o_open=BINANCE.new_order(symbol=symbol, side=open_side, type="MARKET", quantity=qty)
    open_id=int(o_open.get("orderId"))
    techlog({"level":"info","msg":"open_order_ok","symbol":symbol,"side":side,"qty":qty,"order_id":open_id})
    # журнальчик по факту заповнення
    vwap_open,qty_open,fee_open,asset_open,_=_fetch_trades_for_order(symbol, open_id)
    exec_log(signal_id,"OPEN",datetime.now(timezone.utc).isoformat().replace("+00:00","Z"),
             vwap_open,qty_open,fee_open,asset_open,None,symbol,side,open_id)

    # 2) PLACE TP & SL on-exchange (MARK_PRICE)
    exit_side="SELL" if side=="long" else "BUY"
    tp_id=None; sl_id=None
    try:
        oTP=BINANCE.new_order(symbol=symbol, side=exit_side, type="TAKE_PROFIT_MARKET",
                              stopPrice=tp_r, closePosition="true", workingType="MARK_PRICE")
        tp_id=int(oTP.get("orderId"))
        techlog({"level":"info","msg":"tp_order_ok","symbol":symbol,"tp":tp_r,"tp_id":tp_id})
    except Exception as e:
        techlog({"level":"warn","msg":"tp_order_failed","symbol":symbol,"tp":tp_r,"err":str(e)})

    try:
        oSL=BINANCE.new_order(symbol=symbol, side=exit_side, type="STOP_MARKET",
                              stopPrice=sl_r, closePosition="true", workingType="MARK_PRICE")
        sl_id=int(oSL.get("orderId"))
        techlog({"level":"info","msg":"sl_order_ok","symbol":symbol,"sl":sl_r,"sl_id":sl_id})
    except Exception as e:
        techlog({"level":"warn","msg":"sl_order_failed","symbol":symbol,"sl":sl_r,"err":str(e)})

    with BR_LOCK:
        BRACKETS[symbol] = {
            "id":signal_id,"side":side,"tp_id":tp_id,"sl_id":sl_id,
            "open_order_id":open_id,"ts":datetime.now(timezone.utc).isoformat().replace("+00:00","Z")
        }
    techlog({"level":"info","msg":"bracket_seeded","symbol":symbol})

    return {"qty":qty,"price":price,"tp":tp_r,"sl":sl_r,"open_order_id":open_id,"tp_id":tp_id,"sl_id":sl_id}

# ====== DEDUP ======
def build_id(pattern, side, time_raw, entry_val):
    return f"{pattern}|{side}|{str(time_raw).lower()}|{('{:.10f}'.format(float(entry_val)))}"

def dedup_seen(key:str)->bool:
    if key in DEDUP:
        DEDUP.move_to_end(key); return True
    DEDUP[key]=True
    if len(DEDUP)>MAX_KEYS: DEDUP.popitem(last=False)
    return False

# ====== INIT BINANCE & WORKERS ======
if BINANCE_ENABLED and UMFutures:
    try:
        if TESTNET:
            BINANCE = UMFutures(key=API_KEY_TEST, secret=API_SECRET_TEST, base_url="https://testnet.binancefuture.com")
        else:
            BINANCE = UMFutures(key=API_KEY_MAIN, secret=API_SECRET_MAIN)
        techlog({"level":"info","msg":"binance_client_ready","import_path":_BINANCE_IMPORT_PATH,"testnet":TESTNET})
        ensure_oneway_mode()
        for s in PRESET_SYMBOLS:
            try: BINANCE.change_leverage(symbol=s, leverage=LEVERAGE); LEVERAGE_SET.add(s)
            except Exception as e: techlog({"level":"warn","msg":"preset_leverage_failed","symbol":s,"err":str(e)})

        # відновити стан + старт воркерів
        _recover_state()
        threading.Thread(target=_bracket_monitor, daemon=True).start()
        threading.Thread(target=_orphan_sweeper, daemon=True).start()
        techlog({"level":"info","msg":"workers_started","poll_sec":BRACKET_POLL_SEC,"orphan_sec":ORPHAN_SWEEP_SEC})
    except Exception as e:
        techlog({"level":"warn","msg":"binance_client_init_failed","err":str(e)})
        BINANCE_ENABLED=False; BINANCE=None

# ====== ROUTES ======
@app.route("/")
def root(): return "Bot is live", 200

@app.route("/healthz")
def healthz():
    return jsonify({
        "status":"ok","version":"3.1.0-exchangeSL+recovery+orphans",
        "env": os.environ.get("ENV","prod"),
        "trading_enabled": BINANCE_ENABLED,"testnet":TESTNET,
        "risk_mode":RISK_MODE,"risk_pct":RISK_PCT,"leverage":LEVERAGE,
        "preset_symbols":PRESET_SYMBOLS,"binance_import_path":_BINANCE_IMPORT_PATH,
        "poll_sec": BRACKET_POLL_SEC, "orphan_sweep_sec": ORPHAN_SWEEP_SEC,
        "cancel_orphans": CANCEL_ORPHANS, "cancel_retries": CANCEL_RETRIES,
        "time": datetime.now(timezone.utc).isoformat().replace("+00:00","Z")
    })

@app.route("/config", methods=["GET","POST"])
def config():
    global RISK_MODE,RISK_PCT,LEVERAGE
    if request.method=="GET":
        return jsonify({"risk_mode":RISK_MODE,"risk_pct":RISK_PCT,"leverage":LEVERAGE,
                        "allow_pattern":ALLOW_PATTERN,"trading_enabled":BINANCE_ENABLED,"testnet":TESTNET,
                        "replace_on_new":REPLACE_ON_NEW})
    if ADMIN_TOKEN and request.headers.get("X-Admin-Token","")!=ADMIN_TOKEN:
        return jsonify({"status":"error","msg":"unauthorized"}),401
    data=request.get_json(force=True,silent=True) or {}
    if "risk_mode" in data:
        val=str(data["risk_mode"]).lower()
        if val not in ("margin","notional"): return jsonify({"status":"error","msg":"risk_mode must be margin|notional"}),400
        RISK_MODE=val
    if "risk_pct" in data:
        try:
            v=float(data["risk_pct"]); 
            if not (0<v<=100): return jsonify({"status":"error","msg":"risk_pct must be (0;100]"}),400
            RISK_PCT=v
        except: return jsonify({"status":"error","msg":"risk_pct must be float"}),400
    if "leverage" in data:
        try:
            lv=int(data["leverage"]); 
            if not (1<=lv<=125): return jsonify({"status":"error","msg":"leverage out of range"}),400
            LEVERAGE=lv; LEVERAGE_SET.clear()
        except: return jsonify({"status":"error","msg":"leverage must be int"}),400
    techlog({"level":"info","msg":"config_updated","risk_mode":RISK_MODE,"risk_pct":RISK_PCT,"leverage":LEVERAGE})
    return jsonify({"status":"ok","risk_mode":RISK_MODE,"risk_pct":RISK_PCT,"leverage":LEVERAGE})

def validate_payload(d:dict):
    req = ["signal","symbol","time","side","pattern","entry","tp","sl"]
    miss=[k for k in req if k not in d]
    if miss: return False, f"Missing: {','.join(miss)}"
    if str(d["signal"]).lower()!="entry": return False, "signal must be 'entry'"
    if str(d["side"]).lower() not in ("long","short"): return False,"side must be long/short"
    if str(d["pattern"]).lower()!=ALLOW_PATTERN: return False, f"pattern must be '{ALLOW_PATTERN}'"
    e=to_float(d["entry"]); t=to_float(d["tp"]); s=to_float(d["sl"])
    if e is None or t is None or s is None: return False, "entry/tp/sl must be numeric"
    return True, {"entry":e,"tp":t,"sl":s}

@app.route("/webhook", methods=["POST"])
def webhook():
    if not valid_sig(request):
        techlog({"level":"warn","msg":"bad_signature"}); return jsonify({"status":"error","msg":"bad signature"}),401
    try: data=request.get_json(force=True, silent=False)
    except Exception as e:
        techlog({"level":"error","msg":"bad_json","err":str(e)}); return jsonify({"status":"error","msg":"bad json"}),400

    ok, info = validate_payload(data)
    if not ok:
        techlog({"level":"warn","msg":"bad_payload","detail":info,"data":data}); return jsonify({"status":"error","msg":info}),400

    symbol_tv=str(data["symbol"]); symbol=tv_to_binance_symbol(symbol_tv)
    side=str(data["side"]).lower(); pattern=str(data["pattern"]).lower()
    sig_id=build_id(pattern, side, str(data["time"]), float(info["entry"]))

    print(f"[WEBHOOK_OK] id={sig_id} data={json.dumps(data, ensure_ascii=False)}", flush=True)

    # dedup & single-position guard (clean stale if pos=0)
    with BR_LOCK: has_local = symbol in BRACKETS
    if BINANCE and has_local and _position_amt(symbol)==0.0 and not _list_open_orders(symbol):
        with BR_LOCK: BRACKETS.pop(symbol,None)
        techlog({"level":"info","msg":"stale_bracket_purged","symbol":symbol,"id":sig_id}); has_local=False

    if (has_local or (_position_amt(symbol)>0.0)) and not REPLACE_ON_NEW:
        techlog({"level":"info","msg":"ignored_new_signal_active_position","id":sig_id,"symbol":symbol})
        return jsonify({"status":"ok","msg":"ignored_active_position","id":sig_id})

    if dedup_seen(sig_id):
        techlog({"level":"info","msg":"duplicate_ignored","id":sig_id}); return jsonify({"status":"ok","msg":"ignored","id":sig_id})

    # log to CSV
    rotate_if_needed(CSV_PATH)
    newfile=not os.path.exists(CSV_PATH)
    with open(CSV_PATH,"a",newline="",encoding="utf-8") as f:
        w=csv.writer(f); 
        if newfile: w.writerow(["time_raw","time_iso","symbol","pattern","side","entry","tp","sl","id"])
        w.writerow([data["time"], to_iso8601(data["time"]), symbol_tv, pattern, side, info["entry"], info["tp"], info["sl"], sig_id])
    techlog({"level":"info","msg":"logged","id":sig_id,"symbol":symbol_tv,"side":side})

    # trade
    if BINANCE_ENABLED and BINANCE:
        try:
            place_orders_oneway(symbol, side, info["entry"], info["tp"], info["sl"], sig_id)
            techlog({"level":"info","msg":"trade_ok","id":sig_id,"symbol":symbol})
        except Exception as e:
            techlog({"level":"error","msg":"trade_failed","id":sig_id,"err":str(e)})
    else:
        techlog({"level":"info","msg":"trading_disabled","id":sig_id})

    return jsonify({"status":"ok","msg":"logged","id":sig_id})

if __name__=="__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT","5000")))
