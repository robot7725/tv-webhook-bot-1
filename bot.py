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

# дефолт патерну виправлено на "inside"
ALLOW_PATTERN = os.environ.get("ALLOW_PATTERN", "inside").lower()
# прапорець атомарного cancelReplace під час chase
REPRICE_ATOMIC = os.environ.get("REPLACE_ON_NEW", "false").lower() == "true"

# Політика поведінки при наявній позиції: ignore | replace
IN_POSITION_POLICY = os.environ.get("IN_POSITION_POLICY", "ignore").lower()

PRESET_SYMBOLS = [x.strip().upper() for x in os.environ.get("PRESET_SYMBOLS","").split(",") if x.strip()]

SECRET      = os.environ.get("WEBHOOK_SECRET", "")
ADMIN_TOKEN = os.environ.get("ADMIN_TOKEN", "")

LOG_DIR   = os.environ.get("LOG_DIR", "logs")
# дефолт лог-файлу виправлено на inside.csv
LOG_FILE  = os.environ.get("LOG_FILE", "inside.csv")
TECH_LOG  = os.environ.get("TECH_LOG", "tech.jsonl")
EXEC_LOG  = os.environ.get("EXEC_LOG", "executions.csv")
ROTATE_BYTES = int(os.environ.get("ROTATE_BYTES", str(5*1024*1024)))
KEEP_FILES   = int(os.environ.get("KEEP_FILES", "3"))
MAX_KEYS     = int(os.environ.get("DEDUP_CACHE", "2000"))

# Моніторинг/прибирання
BRACKET_POLL_SEC = float(os.environ.get("BRACKET_POLL_SEC", "2"))
CANCEL_ORPHANS   = os.environ.get("CANCEL_ORPHANS", "true").lower() == "true"
ORPHAN_SWEEP_SEC = float(os.environ.get("ORPHAN_SWEEP_SEC", "10"))
CANCEL_RETRIES   = int(os.environ.get("CANCEL_RETRIES", "3"))

# ====== РЕЖИМ ВХОДУ ======
ENTRY_MODE = os.environ.get("ENTRY_MODE", "market").lower()          # market | limit | maker_chase
POST_ONLY  = os.environ.get("POST_ONLY", "true").lower() == "true"   # для LIMIT/CHASE -> timeInForce=GTX

# офсет ціни для пост-онлі (пріоритет має BPS)
PRICE_OFFSET_TICKS = int(os.environ.get("PRICE_OFFSET_TICKS", "0"))
PRICE_OFFSET_BPS   = float(os.environ.get("PRICE_OFFSET_BPS", "0"))

# maker-chase
CHASE_INTERVAL_MS  = int(os.environ.get("CHASE_INTERVAL_MS", "400"))
CHASE_STEPS        = int(os.environ.get("CHASE_STEPS", "10"))
MAX_WAIT_SEC       = float(os.environ.get("MAX_WAIT_SEC", "3"))
MAX_DEVIATION_BPS  = float(os.environ.get("MAX_DEVIATION_BPS", "10"))  # дозволений відрив до фолбеку
FALLBACK = os.environ.get("FALLBACK", "market").lower()               # none | market | limit_ioc

# ====== Звіти (CSV only, без пошти) ======
REPORT_DIR = os.environ.get("REPORT_DIR", os.path.join(LOG_DIR, "reports"))
os.makedirs(LOG_DIR, exist_ok=True)
os.makedirs(REPORT_DIR, exist_ok=True)

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

# ====== ROTATION (safe) ======
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
        try:
            print("[TECH] " + json.dumps(
                {"level":"warn","msg":"rotate_failed","err":str(e),"path":path},
                ensure_ascii=False
            ), flush=True)
        except Exception:
            pass

# ====== UTIL ======
def techlog(entry: dict):
    rotate_if_needed(TECH_PATH)
    entry["ts"] = datetime.now(timezone.utc).isoformat().replace("+00:00","Z")
    with open(TECH_PATH, "a", encoding="utf-8") as f:
        f.write(json.dumps(entry, ensure_ascii=False) + "\n")
    print("[TECH] " + json.dumps(entry, ensure_ascii=False), flush=True)

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

def get_best_bid_ask(symbol):
    bt = BINANCE.book_ticker(symbol=symbol)
    bid = float(bt.get("bidPrice")); ask = float(bt.get("askPrice"))
    return bid, ask

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

def _get_order_exec_qty(symbol, order_id:int) -> float:
    try:
        od = BINANCE.get_order(symbol=symbol, orderId=order_id)
    except:
        od = BINANCE.query_order(symbol=symbol, orderId=order_id)
    v = od.get("executedQty")
    try: return float(v)
    except: return 0.0

def _list_open_orders(symbol=None):
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

# ---- NEW: класифікація відкритих ордерів (entry vs exit) ----
def _split_open_orders(symbol):
    """
    Повертає (entry_orders, exit_orders).
    Exit-ордери: STOP/STOP_MARKET/TAKE_PROFIT/TAKE_PROFIT_MARKET або reduceOnly/closePosition.
    Entry-ордери: усе інше.
    """
    orders = _list_open_orders(symbol) or []
    entries, exits = [], []
    for od in orders:
        typ = str(od.get("type","")).upper()
        ro  = str(od.get("reduceOnly","")).lower() in ("true","1","yes")
        cp  = str(od.get("closePosition","")).lower() in ("true","1","yes")
        if typ in ("STOP","STOP_MARKET","TAKE_PROFIT","TAKE_PROFIT_MARKET") or ro or cp:
            exits.append(od)
        else:
            entries.append(od)
    return entries, exits

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
        print("[HEARTBEAT] " + datetime.now().strftime("%Y-%m-%d %H:%M:%S") + " alive", flush=True)
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

# ====== BRACKET MONITOR ======
def _bracket_monitor():
    while True:
        try:
            with BR_LOCK:
                items=list(BRACKETS.items())
            for symbol, b in items:
                sid=b.get("id"); side=b.get("side")
                tp_id=b.get("tp_id"); sl_id=b.get("sl_id")

                # --- якщо ми плоскі, не зносимо entry-ордера ---
                if _position_amt(symbol)==0.0:
                    entries, exits = _split_open_orders(symbol)

                    if entries:
                        # Є ліміт/чейз на вхід — не чіпаємо нічого.
                        techlog({"level":"debug","msg":"flat_but_entry_present","symbol":symbol,"entries":len(entries),"exits":len(exits)})
                        continue

                    if exits:
                        # Сирітські виходи без позиції та без entry — приберемо тільки їх.
                        for od in exits:
                            oid = int(od.get("orderId", 0))
                            _cancel_order_silent(symbol, oid, "pos_is_zero_exit_cleanup")
                        with BR_LOCK: BRACKETS.pop(symbol, None)
                        techlog({"level":"info","msg":"exit_orphans_cleaned_flat","symbol":symbol,"count":len(exits)})
                        continue

                    # Немає ні entry, ні exit — чистимо локальний стан, якщо зостався.
                    with BR_LOCK: BRACKETS.pop(symbol, None)
                    techlog({"level":"info","msg":"bracket_removed_flat_no_orders","symbol":symbol})
                    continue

                # --- позиція є: слідкуємо за TP/SL ---
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
    candidates = set([s for s in PRESET_SYMBOLS if s])
    try:
        all_open = _list_open_orders(symbol=None) or []
        for od in all_open:
            if "symbol" in od:
                candidates.add(str(od["symbol"]).upper())
    except:
        pass

    for s in sorted(candidates):
        try:
            amt = _position_amt(s)
            orders = _list_open_orders(s) or []
            entries, exits = _split_open_orders(s)

            if amt == 0.0:
                # Ми плоскі: не чіпаємо pending-entries; прибираємо лише сирітські exits (якщо немає entries).
                if entries:
                    techlog({"level":"info","msg":"recover_flat_keep_entries","symbol":s,"entries":len(entries),"exits":len(exits)})
                    continue
                if exits and CANCEL_ORPHANS:
                    for od in exits:
                        _cancel_order_silent(s, int(od.get("orderId",0)), "recover_cleanup_exit_orphans")
                    techlog({"level":"info","msg":"recover_exit_orphans_cleaned","symbol":s,"count":len(exits)})
                continue

            # Позиція є: відновлюємо ідентифікатори TP/SL.
            tp_id = sl_id = None
            for od in orders:
                typ = str(od.get("type","")).upper()
                ro  = str(od.get("reduceOnly","")).lower() in ("true","1","yes")
                cp  = str(od.get("closePosition","")).lower() in ("true","1","yes")
                oid = int(od.get("orderId",0))
                if (typ=="LIMIT" and ro) or typ in ("TAKE_PROFIT","TAKE_PROFIT_MARKET"):
                    tp_id = oid
                if typ in ("STOP","STOP_MARKET") and (cp or ro):
                    sl_id = oid

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
    if not CANCEL_ORPHANS:
        return
    while True:
        try:
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
                        entries, exits = _split_open_orders(s)
                        # лишаємо entry-ордера в спокої; прибираємо тільки сирітські виходи за відсутності entries
                        if (not entries) and exits:
                            for od in exits:
                                _cancel_order_silent(s, int(od.get("orderId",0)), "orphan_sweeper_exit_only")
                            techlog({"level":"info","msg":"orphan_sweeper_exit_cleaned","symbol":s,"count":len(exits)})
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

# ====== ORDER PRICE/OFFSET ======
def _offset_price_from_book(symbol, side, tick):
    bid, ask = get_best_bid_ask(symbol)
    if PRICE_OFFSET_BPS > 0:
        if side=="long":
            base = bid; px = base*(1 - PRICE_OFFSET_BPS/10000.0)
        else:
            base = ask; px = base*(1 + PRICE_OFFSET_BPS/10000.0)
    else:
        off = max(1, PRICE_OFFSET_TICKS)
        if side=="long":
            px = bid - off*tick
        else:
            px = ask + off*tick
    return p_floor_to_tick(px, tick)

# ====== EXIT ORDERS ======
def _place_exits(symbol, side, qty, tp_price, sl_price, signal_id):
    """
    TP як TAKE_PROFIT_MARKET (closePosition=true, workingType=MARK_PRICE).
    SL як STOP_MARKET (closePosition=true).
    Обидва ордери закривають всю позицію незалежно від qty.
    """
    exit_side = "SELL" if side=="long" else "BUY"
    tp_id = sl_id = None
    filt = fetch_symbol_filters(symbol)
    tick = filt.get("tickSize") or 0.0001

    # TP: TAKE_PROFIT_MARKET (closePosition)
    try:
        oTP = BINANCE.new_order(
            symbol=symbol, side=exit_side, type="TAKE_PROFIT_MARKET",
            stopPrice=p_floor_to_tick(tp_price, tick),
            closePosition="true", workingType="MARK_PRICE"
        )
        tp_id = int(oTP.get("orderId"))
        techlog({"level":"info","msg":"tp_take_profit_market_ok","symbol":symbol,"tp":tp_price,"tp_id":tp_id})
    except Exception as e:
        techlog({"level":"warn","msg":"tp_take_profit_market_failed","symbol":symbol,"tp":tp_price,"err":str(e)})

    # SL: STOP_MARKET (closePosition)
    try:
        oSL = BINANCE.new_order(
            symbol=symbol, side=exit_side, type="STOP_MARKET",
            stopPrice=p_floor_to_tick(sl_price, tick),
            closePosition="true", workingType="MARK_PRICE"
        )
        sl_id = int(oSL.get("orderId"))
        techlog({"level":"info","msg":"sl_stop_market_ok","symbol":symbol,"sl":sl_price,"sl_id":sl_id})
    except Exception as e:
        techlog({"level":"warn","msg":"sl_stop_market_failed","symbol":symbol,"sl":sl_price,"err":str(e)})

    with BR_LOCK:
        BRACKETS[symbol] = {
            "id":signal_id,"side":side,"tp_id":tp_id,"sl_id":sl_id,
            "open_order_id":BRACKETS.get(symbol,{}).get("open_order_id",0),
            "ts":datetime.now(timezone.utc).isoformat().replace("+00:00","Z")
        }
    techlog({"level":"info","msg":"bracket_seeded","symbol":symbol})
    return tp_id, sl_id

# ====== ENTRY HELPERS ======
def _entry_market(symbol, side, qty):
    open_side="BUY" if side=="long" else "SELL"
    o_open=BINANCE.new_order(symbol=symbol, side=open_side, type="MARKET", quantity=qty)
    return int(o_open.get("orderId"))

def _entry_limit(symbol, side, qty, price, tif="GTC"):
    filt = fetch_symbol_filters(symbol)
    tick = filt.get("tickSize") or 0.0001
    step = filt.get("stepSize") or 0.001
    price = p_floor_to_tick(price, tick)
    qty   = q_floor_to_step(qty, step)
    open_side="BUY" if side=="long" else "SELL"
    o = BINANCE.new_order(symbol=symbol, side=open_side, type="LIMIT",
                          price=price, quantity=qty, timeInForce=tif)
    return int(o.get("orderId"))

def _status_is_open(status: str) -> bool:
    return status in ("NEW","PARTIALLY_FILLED","PENDING_NEW")

def _within_deviation(entry_ref, side, max_bps):
    bid, ask = get_best_bid_ask(entry_ref["symbol"])
    mkt = ask if side=="short" else bid
    ref = entry_ref["ref_price"]
    if ref<=0: return True
    dev = abs(mkt - ref)/ref*10000.0
    if dev <= max_bps: return True
    techlog({"level":"info","msg":"deviation_exceeded","sym":entry_ref["symbol"],"dev_bps":dev,"max_bps":max_bps})
    return False

def _try_cancel_replace(symbol, side, old_order_id, remain_qty, tif, tick):
    new_price = _offset_price_from_book(symbol, side, tick)
    open_side = "BUY" if side=="long" else "SELL"

    if REPRICE_ATOMIC:
        for m in ("cancel_replace", "cancel_replace_order", "cancelReplace"):
            if hasattr(BINANCE, m):
                try:
                    fn = getattr(BINANCE, m)
                    resp = fn(
                        symbol=symbol,
                        cancelReplaceMode="STOP_ON_FAILURE",
                        cancelOrderId=old_order_id,
                        side=open_side,
                        type="LIMIT",
                        price=p_floor_to_tick(new_price, tick),
                        quantity=remain_qty,
                        timeInForce=tif
                    )
                    new_id = None
                    if isinstance(resp, dict):
                        for k in ("newOrderId", "orderId", "order", "result", "newClientOrderId"):
                            v = resp.get(k)
                            if isinstance(v, dict) and "orderId" in v:
                                new_id = int(v["orderId"]); break
                            if isinstance(v, str) and v.isdigit():
                                new_id = int(v); break
                            if isinstance(v, int):
                                new_id = v; break
                    if new_id:
                        techlog({"level":"info","msg":"entry_reprice_atomic_ok","symbol":symbol,"price":new_price,"remain":remain_qty,"id":new_id})
                        return new_id, True
                except Exception as e:
                    techlog({"level":"warn","msg":"entry_reprice_atomic_failed","symbol":symbol,"err":str(e)})
                    break
    _cancel_order_silent(symbol, old_order_id, "chase_reprice")
    new_id = _entry_limit(symbol, side, remain_qty, new_price, tif=tif)
    techlog({"level":"info","msg":"entry_repriced","symbol":symbol,"price":new_price,"remain":remain_qty,"id":new_id})
    return new_id, False

def _entry_maker_chase(symbol, side, qty, tick, signal_id, ref_price):
    tif = "GTX" if POST_ONLY else "GTC"
    price = _offset_price_from_book(symbol, side, tick)
    order_id = _entry_limit(symbol, side, qty, price, tif=tif)
    techlog({"level":"info","msg":"entry_limit_seeded","symbol":symbol,"side":side,"qty":qty,"price":price,"tif":tif,"id":order_id})

    start = time.time()
    filled_qty = 0.0
    steps_done = 0
    while True:
        time.sleep(max(0.05, CHASE_INTERVAL_MS/1000.0))
        steps_done += 1
        st = _get_order_status(symbol, order_id)
        eq = _get_order_exec_qty(symbol, order_id) if st in ("PARTIALLY_FILLED","FILLED") else 0.0
        filled_qty = max(filled_qty, eq or 0.0)

        if st=="FILLED":
            techlog({"level":"info","msg":"entry_filled","symbol":symbol,"id":order_id,"filled":filled_qty,"steps":steps_done})
            return order_id, filled_qty

        if time.time()-start >= MAX_WAIT_SEC or steps_done >= CHASE_STEPS or not _within_deviation(
            {"symbol":symbol,"ref_price":ref_price}, side, MAX_DEVIATION_BPS):
            break

        remain = max(0.0, qty - (filled_qty or 0.0))
        if remain <= 0:
            return order_id, filled_qty

        try:
            order_id, _ = _try_cancel_replace(symbol, side, order_id, remain, tif, tick)
        except Exception as e:
            techlog({"level":"warn","msg":"entry_reprice_failed","err":str(e)})

    remain = max(0.0, qty - (filled_qty or 0.0))
    if remain > 0:
        try: _cancel_order_silent(symbol, order_id, "fallback")
        except: pass

    if FALLBACK == "market" and remain > 0:
        fb_id = _entry_market(symbol, side, remain)
        techlog({"level":"info","msg":"fallback_market_done","symbol":symbol,"remain":remain,"id":fb_id})
        return fb_id, qty
    elif FALLBACK == "limit_ioc" and remain > 0:
        tif = "IOC"
        fb_price = _offset_price_from_book(symbol, side, tick)
        fb = BINANCE.new_order(symbol=symbol, side=("BUY" if side=="long" else "SELL"), type="LIMIT",
                               price=fb_price, quantity=remain, timeInForce=tif)
        fb_id = int(fb.get("orderId"))
        time.sleep(0.2)
        eq = _get_order_exec_qty(symbol, fb_id)
        techlog({"level":"info","msg":"fallback_limit_ioc_done","symbol":symbol,"filled_ioc":eq,"remain_req":remain,"id":fb_id})
        return fb_id, filled_qty + (eq or 0.0)
    else:
        techlog({"level":"info","msg":"fallback_none","symbol":symbol,"filled":filled_qty,"remain":remain})
        return order_id, filled_qty

def place_orders_oneway(symbol: str, side: str, entry: float, tp: float, sl: float, signal_id: str):
    symbol=symbol.upper()
    ensure_oneway_mode(); ensure_leverage(symbol)

    with BR_LOCK: has_local = symbol in BRACKETS
    if BINANCE and has_local and _position_amt(symbol)==0.0 and not _list_open_orders(symbol):
        with BR_LOCK: BRACKETS.pop(symbol,None)
        techlog({"level":"info","msg":"stale_bracket_purged","symbol":symbol,"id":signal_id}); has_local=False

    pos_amt=_position_amt(symbol)

    if pos_amt>0.0:
        if IN_POSITION_POLICY=="ignore":
            techlog({"level":"info","msg":"ignored_new_signal_active_position","id":signal_id,"symbol":symbol})
            return {"status":"ok","msg":"ignored_active_position","id":signal_id}
        elif IN_POSITION_POLICY=="replace":
            try:
                exit_side="SELL" if side=="long" else "BUY"
                filt=fetch_symbol_filters(symbol); step=filt.get("stepSize") or 0.001
                BINANCE.new_order(symbol=symbol, side=exit_side, type="MARKET", reduceOnly="true",
                                  quantity=q_floor_to_step(pos_amt, step))
                techlog({"level":"info","msg":"replaced_old_position","symbol":symbol,"qty":pos_amt})
            except Exception as e:
                techlog({"level":"warn","msg":"replace_market_close_failed","symbol":symbol,"err":str(e)})
            with BR_LOCK: BRACKETS.pop(symbol, None)

    price_ref = get_mark_price(symbol)
    qty  = compute_qty(symbol, price_ref)
    filt = fetch_symbol_filters(symbol)
    tick = filt.get("tickSize") or 0.0001

    tp_r = p_floor_to_tick(float(tp), tick)
    sl_r = p_floor_to_tick(float(sl), tick)

    # ===== Вхід =====
    if ENTRY_MODE == "market":
        open_id = _entry_market(symbol, side, qty)
    elif ENTRY_MODE == "limit":
        px = _offset_price_from_book(symbol, side, tick)
        tif = "GTX" if POST_ONLY else "GTC"
        open_id = _entry_limit(symbol, side, qty, px, tif=tif)
    else:
        open_id, filled = _entry_maker_chase(symbol, side, qty, tick, signal_id, price_ref)
        if filled <= 0.0 and FALLBACK == "none":
            techlog({"level":"info","msg":"no_entry_filled","symbol":symbol})
            return {"skipped":True,"reason":"no_filled"}
        time.sleep(0.2)
        pos_amt_now = _position_amt(symbol)
        if pos_amt_now > 0:
            qty = pos_amt_now

    with BR_LOCK:
        BRACKETS[symbol] = {
            "id":signal_id,"side":side,"tp_id":None,"sl_id":None,
            "open_order_id":open_id,"ts":datetime.now(timezone.utc).isoformat().replace("+00:00","Z")
        }
    techlog({"level":"info","msg":"open_order_ok","symbol":symbol,"side":side,"qty":qty,"order_id":open_id})

    # Запис OPEN (при LIMIT/CHASE може бути ще не повністю заповнений)
    vwap_open,qty_open,fee_open,asset_open,_=_fetch_trades_for_order(symbol, open_id)
    exec_log(signal_id,"OPEN",datetime.now(timezone.utc).isoformat().replace("+00:00","Z"),
             vwap_open,qty_open,fee_open,asset_open,None,symbol,side,open_id)

    # ===== Виходи =====
    tp_id, sl_id = _place_exits(symbol, side, qty, tp_r, sl_r, signal_id)

    return {"qty":qty,"price_ref":price_ref,"tp":tp_r,"sl":sl_r,"open_order_id":open_id,"tp_id":tp_id,"sl_id":sl_id}

# ====== DEDUP ======
def build_id(pattern, side, time_raw, entry_val):
    entry_str = "{:.10f}".format(float(entry_val))
    return f"{pattern}|{side}|{str(time_raw).lower()}|{entry_str}"

def dedup_seen(key:str)->bool:
    if key in DEDUP:
        DEDUP.move_to_end(key); return True
    DEDUP[key]=True
    if len(DEDUP)>MAX_KEYS: DEDUP.popitem(last=False)
    return False

# ====== REPORT ENDPOINT (CSV only) ======
try:
    import daily_report as DR
except Exception:
    DR = None

from zoneinfo import ZoneInfo
KYIV = ZoneInfo("Europe/Kyiv")

@app.route("/report/daily", methods=["POST"])
def report_daily():
    if ADMIN_TOKEN and request.headers.get("X-Admin-Token","") != ADMIN_TOKEN:
        return jsonify({"status":"error","msg":"unauthorized"}), 401
    if DR is None:
        return jsonify({"status":"error","msg":"daily_report module not found"}), 500

    day = request.args.get("day") or datetime.now(KYIV).strftime("%Y-%m-%d")
    signals_glob = os.path.join(LOG_DIR, "*.csv")
    execs_path   = os.path.join(LOG_DIR, EXEC_LOG)

    try:
        signals = DR.load_signals(signals_glob)
        execs   = DR.load_execs(execs_path)
        daily   = DR.build_daily(signals, execs, day)
        out_path = os.path.join(REPORT_DIR, f"daily_trades_{day}.csv")
        daily.to_csv(out_path, index=False)

        # пошту не шлемо
        sent = False
        return jsonify({"status":"ok","rows":int(daily.shape[0]),"file":out_path,"email_sent":sent})
    except Exception as e:
        techlog({"level":"error","msg":"report_daily_failed","err":str(e)})
        return jsonify({"status":"error","msg":str(e)}), 500

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
        "status":"ok","version":"4.2.2-exit-only-sweep",
        "env": os.environ.get("ENV","prod"),
        "trading_enabled": BINANCE_ENABLED,"testnet":TESTNET,
        "risk_mode":RISK_MODE,"risk_pct":RISK_PCT,"leverage":LEVERAGE,
        "preset_symbols":PRESET_SYMBOLS,"binance_import_path":_BINANCE_IMPORT_PATH,
        "poll_sec": BRACKET_POLL_SEC, "orphan_sweep_sec": ORPHAN_SWEEP_SEC,
        "cancel_orphans": CANCEL_ORPHANS, "cancel_retries": CANCEL_RETRIES,
        "entry_mode": ENTRY_MODE, "post_only": POST_ONLY,
        "offset_ticks": PRICE_OFFSET_TICKS, "offset_bps": PRICE_OFFSET_BPS,
        "chase_ms": CHASE_INTERVAL_MS, "chase_steps": CHASE_STEPS,
        "max_wait_sec": MAX_WAIT_SEC, "max_dev_bps": MAX_DEVIATION_BPS,
        "fallback": FALLBACK,
        "reprice_atomic": REPRICE_ATOMIC,
        "in_position_policy": IN_POSITION_POLICY,
        "log_dir": LOG_DIR, "exec_log": EXEC_LOG, "report_dir": REPORT_DIR,
        "time": datetime.now(timezone.utc).isoformat().replace("+00:00","Z")
    })

@app.route("/config", methods=["GET","POST"])
def config():
    global RISK_MODE,RISK_PCT,LEVERAGE
    if request.method=="GET":
        return jsonify({"risk_mode":RISK_MODE,"risk_pct":RISK_PCT,"leverage":LEVERAGE,
                        "allow_pattern":ALLOW_PATTERN,"trading_enabled":BINANCE_ENABLED,"testnet":TESTNET,
                        "reprice_atomic":REPRICE_ATOMIC,"in_position_policy":IN_POSITION_POLICY,
                        "entry_mode":ENTRY_MODE,"post_only":POST_ONLY,
                        "offset_ticks":PRICE_OFFSET_TICKS,"offset_bps":PRICE_OFFSET_BPS,
                        "chase_ms":CHASE_INTERVAL_MS,"chase_steps":CHASE_STEPS,
                        "max_wait_sec":MAX_WAIT_SEC,"max_dev_bps":MAX_DEVIATION_BPS,"fallback":FALLBACK})
    if ADMIN_TOKEN and request.headers.get("X-Admin-Token","")!=ADMIN_TOKEN:
        return jsonify({"status":"error","msg":"unauthorized"}),401
    data=request.get_json(force=True,silent=True) or {}
    if "risk_mode" in data:
        val=str(data["risk_mode"]).lower()
        if val not in ("margin","notional"): return jsonify({"status":"error","msg":"risk_mode must be margin|notional"}),400
        RISK_MODE=val
    if "risk_pct" in data:
        try:
            v=float(data["risk_pct"])
            if not (0<v<=100): return jsonify({"status":"error","msg":"risk_pct must be (0;100]"}),400
            RISK_PCT=v
        except: return jsonify({"status":"error","msg":"risk_pct must be float"}),400
    if "leverage" in data:
        try:
            lv=int(data["leverage"])
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

    print("[WEBHOOK_OK] id={} data={}".format(sig_id, json.dumps(data, ensure_ascii=False)), flush=True)

    with BR_LOCK: has_local = symbol in BRACKETS
    if BINANCE and has_local and _position_amt(symbol)==0.0 and not _list_open_orders(symbol):
        with BR_LOCK: BRACKETS.pop(symbol,None)
        techlog({"level":"info","msg":"stale_bracket_purged","symbol":symbol,"id":sig_id}); has_local=False

    if (_position_amt(symbol)>0.0) and IN_POSITION_POLICY=="ignore":
        techlog({"level":"info","msg":"ignored_new_signal_active_position","id":sig_id,"symbol":symbol})
        return jsonify({"status":"ok","msg":"ignored_active_position","id":sig_id})

    if dedup_seen(sig_id):
        techlog({"level":"info","msg":"duplicate_ignored","id":sig_id}); return jsonify({"status":"ok","msg":"ignored","id":sig_id})

    rotate_if_needed(CSV_PATH)
    newfile=not os.path.exists(CSV_PATH)
    with open(CSV_PATH,"a",newline="",encoding="utf-8") as f:
        w=csv.writer(f)
        if newfile: w.writerow(["time_raw","time_iso","symbol","pattern","side","entry","tp","sl","id"])
        w.writerow([data["time"], to_iso8601(data["time"]), symbol_tv, pattern, side, info["entry"], info["tp"], info["sl"], sig_id])
    techlog({"level":"info","msg":"logged","id":sig_id,"symbol":symbol_tv,"side":side})

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
