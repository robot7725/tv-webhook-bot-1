# bot.py
import os, csv, threading
from flask import Flask, request, jsonify
from collections import OrderedDict

app = Flask(__name__)
LOCK = threading.Lock()

# === ENV ===
LOG_DIR = os.environ.get("LOG_DIR", "logs")
os.makedirs(LOG_DIR, exist_ok=True)
LOG_FILE = os.path.join(LOG_DIR, os.environ.get("LOG_FILE", "engulfing.csv"))
ALLOW_PATTERN = os.environ.get("ALLOW_PATTERN", "engulfing").lower()

# === Простий LRU-дедуп по ключу symbol|time|side|pattern ===
MAX_KEYS = 1000
DEDUP = OrderedDict()

def _key(symbol, time_s, side, pattern):
    return f"{symbol}|{time_s}|{side}|{pattern}".lower()

def _seen(key):
    with LOCK:
        if key in DEDUP:
            DEDUP.move_to_end(key)
            return True
        DEDUP[key] = True
        if len(DEDUP) > MAX_KEYS:
            DEDUP.popitem(last=False)
        return False

def _to_float(x):
    try:
        return float(x)
    except Exception:
        return None

def _validate(d):
    req = ["signal","symbol","time","side","pattern","entry","tp","sl"]
    miss = [k for k in req if k not in d]
    if miss:
        return False, f"Missing fields: {','.join(miss)}"
    if str(d["signal"]).lower() != "entry":
        return False, "signal must be 'entry'"
    if str(d["pattern"]).lower() != ALLOW_PATTERN:
        return False, f"pattern must be '{ALLOW_PATTERN}' for this bot"
    if str(d["side"]).lower() not in ("long","short"):
        return False, "side must be long/short"

    entry = _to_float(d["entry"]); tp = _to_float(d["tp"]); sl = _to_float(d["sl"])
    if entry is None or tp is None or sl is None:
        return False, "entry/tp/sl must be numeric"
    return True, {"entry": entry, "tp": tp, "sl": sl}

@app.route("/webhook", methods=["POST"])
def webhook():
    try:
        data = request.get_json(force=True, silent=False)
    except Exception as e:
        return jsonify({"status":"error","msg":f"bad json: {e}"}), 400

    ok, info = _validate(data or {})
    if not ok:
        return jsonify({"status":"error","msg":info}), 400

    symbol  = str(data["symbol"])
    time_s  = str(data["time"])      # беремо як є (ms або ISO — однаково)
    side    = str(data["side"]).lower()
    pattern = str(data["pattern"]).lower()
    key = _key(symbol, time_s, side, pattern)

    if _seen(key):
        return jsonify({"status":"ok","msg":"duplicate ignored"}), 200

    row = [time_s, symbol, pattern, side, info["entry"], info["tp"], info["sl"]]

    with LOCK:
        newfile = not os.path.exists(LOG_FILE)
        with open(LOG_FILE, "a", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            if newfile:
                w.writerow(["time","symbol","pattern","side","entry","tp","sl"])
            w.writerow(row)

    return jsonify({"status":"ok","msg":"logged"}), 200

@app.route("/", methods=["GET"])
def root():
    return "OK", 200

# Render сам підставляє PORT і запускає через gunicorn (див. Procfile)
