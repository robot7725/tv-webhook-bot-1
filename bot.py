import os, csv, hmac, hashlib, json, threading, time, re, math
from decimal import Decimal, ROUND_DOWN
from collections import OrderedDict
from datetime import datetime, timezone
from flask import Flask, request, jsonify

# ==== Binance (USDT-M Futures) ====
BINANCE_ENABLED = os.environ.get("TRADING_ENABLED", "false").lower() == "true"
if BINANCE_ENABLED:
    try:
        from binance.um_futures import UMFutures
    except Exception as e:
        raise RuntimeError("Missing dependency 'binance-connector'. Add to requirements.txt") from e

app = Flask(__name__)

# ===== ENV / config =====
LOG_DIR   = os.environ.get("LOG_DIR", "logs")
LOG_FILE  = os.environ.get("LOG_FILE", "engulfing.csv")
TECH_LOG  = os.environ.get("TECH_LOG", "tech.jsonl")
SECRET    = os.environ.get("WEBHOOK_SECRET", "")      # якщо порожній — перевірка підпису вимкнена
ALLOW_PATTERN = os.environ.get("ALLOW_PATTERN", "engulfing").lower()
ENV_MODE  = os.environ.get("ENV", "prod")
MAX_KEYS  = int(os.environ.get("DEDUP_CACHE", "2000"))          # кеш ідемпотентності
ROTATE_BYTES = int(os.environ.get("ROTATE_BYTES", str(5*1024*1024)))  # 5MB
KEEP_FILES   = int(os.environ.get("KEEP_FILES", "3"))

# Binance trade settings
TESTNET     = os.environ.get("TESTNET", "true").lower() == "true"
API_KEY     = os.environ.get("BINANCE_API_KEY", "")
API_SECRET  = os.environ.get("BINANCE_API_SECRET", "")
LEVERAGE    = int(os.environ.get("LEVERAGE", "10"))
RISK_MODE   = os.environ.get("RISK_MODE", "margin").lower()       # margin | notional
RISK_PCT    = float(os.environ.get("RISK_PCT", "1.0"))            # % балансу
ADMIN_TOKEN = os.environ.get("ADMIN_TOKEN", "")                   # для POST /config (опційно)

os.makedirs(LOG_DIR, exist_ok=True)
CSV_PATH  = os.path.join(LOG_DIR, LOG_FILE)
TECH_PATH = os.path.join(LOG_DIR, TECH_LOG)

# ===== utils =====
DEDUP = OrderedDict()

def techlog(entry: dict):
    entry["ts"] = datetime.now(timezone.utc).isoformat().replace("+00:00","Z")
    with open(TECH_PATH, "a", encoding="utf-8") as f:
        f.write(json.dumps(entry, ensure_ascii=False) + "\n")
    # дублюємо у stdout для Live logs
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
    """
    Приймає epoch (сек/мс) або ISO (з/без 'Z'). Повертає ISO UTC.
    """
    s = str(ts_str).strip()
    # число (сек/мс)
    try:
        v = float(s); iv = int(v)
        if iv > 10_000_000_000:   # мс
            return datetime.fromtimestamp(iv/1000, tz=timezone.utc).isoformat().replace("+00:00","Z")
        if iv > 1_000_000_000:    # сек
            return datetime.fromtimestamp(iv, tz=timezone.utc).isoformat().replace("+00:00","Z")
    except Exception:
        pass
    # ISO
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

# ===== heartbeat (для 24/7 моніторингу) =====
def _heartbeat():
    while True:
        print(f"[HEARTBEAT] {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} alive", flush=True)
        time.sleep(60)

threading.Thread(target=_heartbeat, daemon=True).start()
print("[BOOT] bot process started", flush=True)

# ===== Binance helpers (One-way) =====
BINANCE = None
SYMBOL_CACHE = {}      # symbol -> filters {stepSize,tickSize,minQty,minNotional}
LEVERAGE_SET = set()   # symbols where leverage already set

def tv_to_binance_symbol(tv_symbol: str) -> str:
    # Напр.: 1000PEPEUSDT.P -> 1000PEPEUSDT
    return re.sub(r"\.P$", "", str(tv_symbol))

def d_floor(x: float, step: float) -> float:
    d = Decimal(str(x))
    s = Decimal(str(step))
    return float((d // s) * s)

def q_floor_to_step(qty: float, step: float) -> float:
    # Безпечно округлюємо вниз під stepSize
    if step <= 0:
        return qty
    return float((Decimal(str(qty)) / Decimal(str(step))).to_integral_value(rounding=ROUND_DOWN) * Decimal(str(step)))

def p_round_to_tick(price: float, tick: float) -> float:
    # Округлюємо вниз під tickSize (щоб не зловити PRICE_FILTER)
    if tick <= 0:
        return price
    return float((Decimal(str(price))).quantize(Decimal(str(tick)), rounding=ROUND_DOWN))

def fetch_symbol_filters(symbol: str):
    if symbol in SYMBOL_CACHE:
        return SYMBOL_CACHE[symbol]
    info = BINANCE.exchange_info()  # кешуємо 1 раз
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
    bals = BINANCE.balance()  # [{asset, balance, availableBalance, ...}]
    for b in bals:
        if b.get("asset") == "USDT":
            return float(b.get("availableBalance"))
    raise RuntimeError("USDT balance not found")

def get_mark_price(symbol: str) -> float:
    mp = BINANCE.mark_price(symbol=symbol)
    return float(mp["markPrice"])

def ensure_oneway_mode():
    try:
        BINANCE.change_position_mode(dualSidePosition="false")  # one-way
    except Exception as e:
        techlog({"level":"warn","msg":"change_position_mode_failed","err":str(e)})

def ensure_leverage(symbol: str):
    if symbol in LEVERAGE_SET:
        return
    try:
        BINANCE.change_leverage(symbol=symbol, leverage=LEVERAGE)
        LEVERAGE_SET.add(symbol)
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

def place_orders_oneway(symbol: str, side: str, entry: float, tp: float, sl: float):
    """
    Маркет-вхід + два closePosition тригери (STOP/TP) для one-way.
    """
    ensure_oneway_mode()
    ensure_leverage(symbol)

    price = get_mark_price(symbol)
    qty   = compute_qty(symbol, price)

    # округлення SL/TP під tickSize
    filt = fetch_symbol_filters(symbol)
    tick = filt["tickSize"] or 0.0001
    sl_r = p_round_to_tick(sl, tick)
    tp_r = p_round_to_tick(tp, tick)

    # 1) Market entry
    open_side = "BUY" if side == "long" else "SELL"
    o1 = BINANCE.new_order(symbol=symbol, side=open_side, type="MARKET", quantity=qty)

    # 2) SL/TP triggers (closePosition=true) по MARK_PRICE
    exit_side = "SELL" if side == "long" else "BUY"

    # SL
    BINANCE.new_order(
        symbol=symbol, side=exit_side, type="STOP_MARKET",
        stopPrice=sl_r, closePosition="true", workingType="MARK_PRICE"
    )
    # TP
    BINANCE.new_order(
        symbol=symbol, side=exit_side, type="TAKE_PROFIT_MARKET",
        stopPrice=tp_r, closePosition="true", workingType="MARK_PRICE"
    )

    print(f"[TRADE] {symbol} {side} qty={qty} price={price} tp={tp_r} sl={sl_r}", flush=True)
    techlog({"level":"info","msg":"binance_orders_placed","symbol":symbol,"qty":qty,"open_side":open_side,"tp":tp_r,"sl":sl_r,"price":price,"mode":"oneway"})
    return {"qty":qty, "price":price, "tp":tp_r, "sl":sl_r, "open_order":o1}

# ===== Binance init =====
BINANCE = None
if BINANCE_ENABLED:
    base_url = "https://testnet.binancefuture.com" if TESTNET else None
    BINANCE = UMFutures(api_key=API_KEY, api_secret=API_SECRET, base_url=base_url)

# ===== routes =====
@app.route("/", methods=["GET"])
def root():
    return "Bot is live", 200

@app.route("/healthz", methods=["GET"])
def healthz():
    return jsonify({
        "status":"ok",
        "version":"2.1.0",
        "env":ENV_MODE,
        "trading_enabled": BINANCE_ENABLED,
        "testnet": TESTNET,
        "risk_mode": RISK_MODE,
        "risk_pct": RISK_PCT,
        "leverage": LEVERAGE,
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
    # POST
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
            LEVERAGE_SET.clear()  # перевстановимо при наступному ордері
        except Exception:
            return jsonify({"status":"error","msg":"leverage must be int"}), 400
    techlog({"level":"info","msg":"config_updated","risk_mode":RISK_MODE,"risk_pct":RISK_PCT,"leverage":LEVERAGE})
    return jsonify({"status":"ok","risk_mode":RISK_MODE,"risk_pct":RISK_PCT,"leverage":LEVERAGE}), 200

# ---- Webhook ----
@app.route("/webhook", methods=["POST"])
def webhook():
    # видимий лог запиту до будь-якої перевірки
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

    # видимий лог корисного навантаження
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

    # --- Binance trade (optional) ---
    if BINANCE_ENABLED:
        try:
            if not BINANCE:
                raise RuntimeError("BINANCE client is not initialized")
            symbol = tv_to_binance_symbol(symbol_tv)
            res = place_orders_oneway(symbol=symbol, side=side, entry=info["entry"], tp=info["tp"], sl=info["sl"])
            techlog({"level":"info","msg":"trade_ok","id":sig_id,"binance":res})
        except Exception as e:
            techlog({"level":"error","msg":"trade_failed","id":sig_id,"err":str(e)})
            # Не валимо вебхук: логи вже записані
    else:
        techlog({"level":"info","msg":"trading_disabled","id":sig_id})

    return jsonify({"status":"ok","msg":"logged","id":sig_id}), 200

# локальний запуск (у Render стартує через Gunicorn)
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", "5000")))
