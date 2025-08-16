import os, csv, hmac, hashlib, json, threading, time, re
from decimal import Decimal, ROUND_DOWN
from collections import OrderedDict
from datetime import datetime, timezone
from flask import Flask, request, jsonify

# ==== Binance (USDT-M Futures) ====
BINANCE_ENABLED = os.environ.get("TRADING_ENABLED", "false").lower() == "true"

UMFutures = None
_BINANCE_IMPORT_PATH = None
if BINANCE_ENABLED:
    # Підтримка різних структур пакета у різних версіях binance-connector
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
            print("[WARN] Cannot import UMFutures; trading disabled:",
                  repr(e1), "|", repr(e2), flush=True)
            BINANCE_ENABLED = False
            UMFutures = None

app = Flask(__name__)

# ===== ENV / config =====
LOG_DIR   = os.environ.get("LOG_DIR", "logs")
LOG_FILE  = os.environ.get("LOG_FILE", "engulfing.csv")  # змініть на inside.csv за бажанням
TECH_LOG  = os.environ.get("TECH_LOG", "tech.jsonl")
SECRET    = os.environ.get("WEBHOOK_SECRET", "")
ALLOW_PATTERN = os.environ.get("ALLOW_PATTERN", "engulfing").lower()
ENV_MODE  = os.environ.get("ENV", "prod")
MAX_KEYS  = int(os.environ.get("DEDUP_CACHE", "2000"))
ROTATE_BYTES = int(os.environ.get("ROTATE_BYTES", str(5*1024*1024)))  # 5MB
KEEP_FILES   = int(os.environ.get("KEEP_FILES", "3"))

# Binance trade settings
TESTNET      = os.environ.get("TESTNET", "true").lower() == "true"
API_KEY      = os.environ.get("BINANCE_API_KEY", "")
API_SECRET   = os.environ.get("BINANCE_API_SECRET", "")
LEVERAGE     = int(os.environ.get("LEVERAGE", "10"))
RISK_MODE    = os.environ.get("RISK_MODE", "margin").lower()       # margin | notional
RISK_PCT     = float(os.environ.get("RISK_PCT", "1.0"))            # % балансу
ADMIN_TOKEN  = os.environ.get("ADMIN_TOKEN", "")
PRESET_SYMBOLS = os.environ.get("PRESET_SYMBOLS", "")              # "1000PEPEUSDT,BTCUSDT"

# Bracket monitor settings
CANCEL_ORPHANS   = os.environ.get("CANCEL_ORPHANS", "true").lower() == "true"
BRACKET_POLL_SEC = float(os.environ.get("BRACKET_POLL_SEC", "1"))

os.makedirs(LOG_DIR, exist_ok=True)
CSV_PATH  = os.path.join(LOG_DIR, LOG_FILE)
TECH_PATH = os.path.join(LOG_DIR, TECH_LOG)

# ===== utils =====
DEDUP = OrderedDict()
BRACKETS = {}  # symbol -> {"side": "long|short", "tp_id": int, "sl_id": int, "ts": iso}

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
                if i == KEEP_FILES:
                    os.remove(src)
                else:
                    os.rename(src, dst)
        os.rename(path, f"{path}.1")
    except Exception as e:
        techlog({"level":"warn","msg":"rotate_failed","err":str(e),"path":path})

def to_float(x):
    try:
        return float(x)
    except Exception:
        return None

def to_iso8601(ts_str: str) -> str:
    """Приймає epoch (сек/мс) або ISO (з/без 'Z'). Повертає ISO UTC."""
    s = str(ts_str).strip()
    try:
        v = float(s); iv = int(v)
        if iv > 10_000_000_000:   # мс
            return datetime.fromtimestamp(iv/1000, tz=timezone.utc).isoformat().replace("+00:00","Z")
        if iv > 1_000_000_000:    # сек
            return datetime.fromtimestamp(iv, tz=timezone.utc).isoformat().replace("+00:00","Z")
    except Exception:
        pass
    try:
        if s.endswith("Z"): s = s[:-1] + "+00:00"
        return datetime.fromisoformat(s).astimezone(timezone.utc).isoformat().replace("+00:00","Z")
    except Exception:
        return datetime.now(timezone.utc).isoformat().replace("+00:00","Z")

def calc_sig(raw_body: bytes) -> str:
    if not SECRET:
        return ""
    return hmac.new(SECRET.encode("utf-8"), raw_body, hashlib.sha256).hexdigest()

def valid_sig(req) -> bool:
    if not SECRET:
        return True
    header = req.headers.get("X-Signature", "")
    try:
        raw = req.get_data(cache=False, as_text=False)
    except Exception:
        return False
    return hmac.compare_digest(header, calc_sig(raw))

def validate_payload(d: dict):
    required = ["signal","symbol","time","side","pattern","entry","tp","sl"]
    miss = [k for k in required if k not in d]
    if miss:
        return False, f"Missing fields: {','.join(miss)}"
    if str(d["signal"]).lower() != "entry":
        return False, "signal must be 'entry'"
    if str(d["side"]).lower() not in ("long","short"):
        return False, "side must be long/short"
    if str(d["pattern"]).lower() != ALLOW_PATTERN:
        return False, f"pattern must be '{ALLOW_PATTERN}'"
    entry = to_float(d["entry"]); tp = to_float(d["tp"]); sl = to_float(d["sl"])
    if entry is None or tp is None or sl is None:
        return False, "entry/tp/sl must be numeric"
    return True, {"entry":entry, "tp":tp, "sl":sl}

def build_id(pattern: str, side: str, time_raw: str) -> str:
    return f"{pattern}|{side}|{str(time_raw).lower()}"

def dedup_seen(key: str) -> bool:
    if key in DEDUP:
        DEDUP.move_to_end(key)
        return True
    DEDUP[key] = True
    if len(DEDUP) > MAX_KEYS:
        DEDUP.popitem(last=False)
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
SYMBOL_CACHE = {}         # symbol -> {stepSize,tickSize,minQty,minNotional}
LEVERAGE_SET = set()      # де вже встановили плече в цій сесії

def tv_to_binance_symbol(tv_symbol: str) -> str:
    # 1000PEPEUSDT.P -> 1000PEPEUSDT
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
    if symbol in SYMBOL_CACHE:
        return SYMBOL_CACHE[symbol]
    info = BINANCE.exchange_info()
    filt = {"stepSize":None, "tickSize":None, "minQty":None, "minNotional":None}
    for s in info.get("symbols", []):
        if s.get("symbol") == symbol:
            for f in s.get("filters", []):
                t = f.get("filterType")
                if t == "LOT_SIZE":
                    filt["stepSize"] = float(f["stepSize"])
                    filt["minQty"] = float(f["minQty"])
                elif t == "PRICE_FILTER":
                    filt["tickSize"] = float(f["tickSize"])
                elif t in ("MIN_NOTIONAL","NOTIONAL"):
                    mn = f.get("notional") or f.get("minNotional")
                    if mn is not None:
                        filt["minNotional"] = float(mn)
            SYMBOL_CACHE[symbol] = filt
            return filt
    raise ValueError(f"Symbol {symbol} not found in exchangeInfo")

def get_available_balance_usdt() -> float:
    bals = BINANCE.balance()
    for b in bals:
        if b.get("asset") == "USDT":
            return float(b.get("availableBalance"))
    raise RuntimeError("USDT balance not found")

def get_mark_price(symbol: str) -> float:
    mp = BINANCE.mark_price(symbol=symbol.upper())
    return float(mp["markPrice"])

def ensure_oneway_mode():
    global ONEWAY_SET
    if ONEWAY_SET:
        return
    try:
        BINANCE.change_position_mode(dualSidePosition="false")
        ONEWAY_SET = True
        techlog({"level":"info","msg":"oneway_mode_ok"})
    except Exception as e:
        techlog({"level":"warn","msg":"change_position_mode_failed","err":str(e)})

def ensure_leverage(symbol: str):
    symbol = symbol.upper()
    if symbol in LEVERAGE_SET:
        return
    try:
        BINANCE.change_leverage(symbol=symbol, leverage=LEVERAGE)
        LEVERAGE_SET.add(symbol)
        techlog({"level":"info","msg":"leverage_ok","symbol":symbol,"leverage":LEVERAGE})
    except Exception as e:
        techlog({"level":"warn","msg":"change_leverage_failed","symbol":symbol,"err":str(e)})

def compute_qty(symbol: str, price: float) -> float:
    bal = get_available_balance_usdt()
    if RISK_MODE == "margin":
        margin = bal * (RISK_PCT / 100.0)
        notional = margin * LEVERAGE
    else:
        notional = bal * (RISK_PCT / 100.0)
    qty_raw = notional / price

    filt = fetch_symbol_filters(symbol)
    step = filt["stepSize"] or 0.001
    min_qty = filt["minQty"] or 0.0
    min_not = filt["minNotional"] or 0.0

    qty = q_floor_to_step(qty_raw, step)
    if min_qty and qty < min_qty:
        qty = min_qty
    if min_not and (qty * price) < min_not:
        qty = q_floor_to_step((min_not / price), step) or min_qty

    if qty <= 0:
        raise RuntimeError("Computed qty <= 0. Increase RISK_PCT or leverage.")
    return qty

def _get_order_status(symbol: str, order_id: int) -> str:
    """Повертає статус ордера ('NEW','FILLED','CANCELED',...)."""
    try:
        od = BINANCE.get_order(symbol=symbol, orderId=order_id)
    except Exception:
        # деякі збірки використовують інші назви
        od = BINANCE.query_order(symbol=symbol, orderId=order_id)
    return str(od.get("status", "")).upper()

def _cancel_order_silent(symbol: str, order_id: int, reason: str):
    if not order_id:
        return
    try:
        BINANCE.cancel_order(symbol=symbol, orderId=order_id)
        techlog({"level":"info","msg":"cancel_order","symbol":symbol,"order_id":order_id,"reason":reason})
    except Exception as e:
        techlog({"level":"warn","msg":"cancel_order_failed","symbol":symbol,"order_id":order_id,"err":str(e)})

def _position_amt(symbol: str) -> float:
    """One-way: повертає абсолютний розмір позиції за символом."""
    try:
        data = BINANCE.position_risk(symbol=symbol)
    except Exception:
        data = BINANCE.position_information(symbol=symbol)
    amt = 0.0
    for p in data:
        if p.get("symbol") == symbol:
            try:
                amt = float(p.get("positionAmt") or p.get("positionAmt".upper(), 0.0))
            except Exception:
                amt = 0.0
            break
    return abs(amt)

def _bracket_monitor():
    """Фоновий потік: якщо TP або SL виконано — скасовуємо «брата».
       Якщо позиція 0 — прибираємо всі open orders по символу."""
    while True:
        try:
            for symbol, b in list(BRACKETS.items()):
                tp_id = b.get("tp_id")
                sl_id = b.get("sl_id")

                tp_st = _get_order_status(symbol, tp_id) if tp_id else ""
                sl_st = _get_order_status(symbol, sl_id) if sl_id else ""

                if tp_st == "FILLED" and sl_st not in ("CANCELED","FILLED","EXPIRED"):
                    _cancel_order_silent(symbol, sl_id, reason="sibling_tp_filled")
                    BRACKETS.pop(symbol, None)
                    continue

                if sl_st == "FILLED" and tp_st not in ("CANCELED","FILLED","EXPIRED"):
                    _cancel_order_silent(symbol, tp_id, reason="sibling_sl_filled")
                    BRACKETS.pop(symbol, None)
                    continue

                # Catch-all: позиція закрита — прибрати всі відкриті ордери
                if _position_amt(symbol) == 0.0:
                    try:
                        BINANCE.cancel_all_open_orders(symbol=symbol)
                        techlog({"level":"info","msg":"cancel_all_on_zero_pos","symbol":symbol})
                    except Exception as e:
                        techlog({"level":"warn","msg":"cancel_all_failed","symbol":symbol,"err":str(e)})
                    BRACKETS.pop(symbol, None)
        except Exception as e:
            techlog({"level":"warn","msg":"bracket_monitor_error","err":str(e)})
        time.sleep(BRACKET_POLL_SEC)

def place_orders_oneway(symbol: str, side: str, entry: float, tp: float, sl: float):
    """Маркет-вхід + два closePosition тригери (STOP/TP) для one-way."""
    symbol = symbol.upper()
    ensure_oneway_mode()
    ensure_leverage(symbol)

    # Якщо вже є активна «скоба» по символу — акуратно приберемо її
    if symbol in BRACKETS:
        b = BRACKETS.get(symbol, {})
        _cancel_order_silent(symbol, b.get("tp_id"), reason="replace_bracket_tp")
        _cancel_order_silent(symbol, b.get("sl_id"), reason="replace_bracket_sl")
        BRACKETS.pop(symbol, None)

    price = get_mark_price(symbol)
    qty   = compute_qty(symbol, price)

    filt = fetch_symbol_filters(symbol)
    tick = filt["tickSize"] or 0.0001
    sl_r = p_floor_to_tick(sl, tick)
    tp_r = p_floor_to_tick(tp, tick)

    open_side = "BUY" if side == "long" else "SELL"
    o1 = BINANCE.new_order(symbol=symbol, side=open_side, type="MARKET", quantity=qty)

    exit_side = "SELL" if side == "long" else "BUY"
    # SL
    oSL = BINANCE.new_order(
        symbol=symbol, side=exit_side, type="STOP_MARKET",
        stopPrice=sl_r, closePosition="true", workingType="MARK_PRICE"
    )
    # TP
    oTP = BINANCE.new_order(
        symbol=symbol, side=exit_side, type="TAKE_PROFIT_MARKET",
        stopPrice=tp_r, closePosition="true", workingType="MARK_PRICE"
    )

    # Запам'ятати скобу
    BRACKETS[symbol] = {
        "side": side,
        "tp_id": int(oTP.get("orderId")),
        "sl_id": int(oSL.get("orderId")),
        "ts": datetime.now(timezone.utc).isoformat().replace("+00:00","Z")
    }

    print(f"[TRADE] {symbol} {side} qty={qty} price={price} tp={tp_r} sl={sl_r}", flush=True)
    techlog({"level":"info","msg":"binance_orders_placed",
             "symbol":symbol,"qty":qty,"open_side":open_side,"tp":tp_r,"sl":sl_r,
             "tp_id":BRACKETS[symbol]["tp_id"],"sl_id":BRACKETS[symbol]["sl_id"],
             "price":price,"mode":"oneway"})
    return {"qty":qty, "price":price, "tp":tp_r, "sl":sl_r, "open_order":o1}

# ===== Binance init =====
BINANCE = None
if BINANCE_ENABLED and UMFutures:
    try:
        if TESTNET:
            BINANCE = UMFutures(key=API_KEY, secret=API_SECRET,
                                base_url="https://testnet.binancefuture.com")
        else:
            BINANCE = UMFutures(key=API_KEY, secret=API_SECRET)
        techlog({"level":"info","msg":"binance_client_ready",
                 "import_path":_BINANCE_IMPORT_PATH,"testnet":TESTNET})
        # Пресет режими
        if PRESET_SYMBOLS:
            ensure_oneway_mode()
            for s in [x.strip().upper() for x in PRESET_SYMBOLS.split(",") if x.strip()]:
                try:
                    BINANCE.change_leverage(symbol=s, leverage=LEVERAGE)
                    LEVERAGE_SET.add(s)
                    techlog({"level":"info","msg":"preset_leverage_ok","symbol":s,"leverage":LEVERAGE})
                except Exception as e:
                    techlog({"level":"warn","msg":"preset_leverage_failed","symbol":s,"err":str(e)})
        # Запуск монітора «скоб»
        if CANCEL_ORPHANS:
            threading.Thread(target=_bracket_monitor, daemon=True).start()
            techlog({"level":"info","msg":"bracket_monitor_started","poll_sec":BRACKET_POLL_SEC})
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
        "version":"2.3.0",
        "env":ENV_MODE,
        "trading_enabled": BINANCE_ENABLED,
        "testnet": TESTNET,
        "risk_mode": RISK_MODE,
        "risk_pct": RISK_PCT,
        "leverage": LEVERAGE,
        "preset_symbols": [x.strip().upper() for x in PRESET_SYMBOLS.split(",") if x.strip()],
        "binance_import_path": _BINANCE_IMPORT_PATH,
        "cancel_orphans": CANCEL_ORPHANS,
        "poll_sec": BRACKET_POLL_SEC,
        "time": datetime.now(timezone.utc).isoformat().replace("+00:00","Z")
    }), 200

# ---- Simple config panel (JSON API) ----
@app.route("/config", methods=["GET", "POST"])
def config():
    global RISK_MODE, RISK_PCT, LEVERAGE
    if request.method == "GET":
        return jsonify({
            "risk_mode": RISK_MODE,
            "risk_pct": RISK_PCT,
            "leverage": LEVERAGE,
            "allow_pattern": ALLOW_PATTERN,
            "trading_enabled": BINANCE_ENABLED,
            "testnet": TESTNET
        }), 200
    # POST (захист)
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
    techlog({"level":"info","msg":"config_updated",
             "risk_mode":RISK_MODE,"risk_pct":RISK_PCT,"leverage":LEVERAGE})
    return jsonify({"status":"ok","risk_mode":RISK_MODE,"risk_pct":RISK_PCT,"leverage":LEVERAGE}), 200

# ---- Manual leverage setter
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
            LEVERAGE_SET.add(sym)
            done.append(sym)
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
    sig_id     = build_id(pattern, side, time_s)

    print(f"[WEBHOOK_OK] id={sig_id} data={json.dumps(data, ensure_ascii=False)}", flush=True)

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
            symbol = tv_to_binance_symbol(symbol_tv)
            place_orders_oneway(symbol=symbol, side=side, entry=info["entry"], tp=info["tp"], sl=info["sl"])
            techlog({"level":"info","msg":"trade_ok","id":sig_id,"symbol":symbol})
        except Exception as e:
            techlog({"level":"error","msg":"trade_failed","id":sig_id,"err":str(e)})
    else:
        techlog({"level":"info","msg":"trading_disabled","id":sig_id})

    return jsonify({"status":"ok","msg":"logged","id":sig_id}), 200

# локальний запуск (у Render стартує через Gunicorn)
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", "5000")))
