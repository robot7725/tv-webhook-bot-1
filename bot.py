# bot.py
import os, csv
from collections import OrderedDict
from datetime import datetime, timezone
from flask import Flask, request, jsonify

app = Flask(__name__)

# === ENV ===
LOG_DIR = os.environ.get("LOG_DIR", "logs")
LOG_FILE = os.environ.get("LOG_FILE", "engulfing.csv")
ALLOW_PATTERN = os.environ.get("ALLOW_PATTERN", "engulfing").lower()  # цей деплой під Поглинання

os.makedirs(LOG_DIR, exist_ok=True)
LOG_PATH = os.path.join(LOG_DIR, LOG_FILE)

# === невеликий LRU для дедупу: уникаємо повторів того ж сигналу (symbol|time|side|pattern) ===
MAX_KEYS = 1000
DEDUP = OrderedDict()

def seen(key: str) -> bool:
    if key in DEDUP:
        DEDUP.move_to_end(key)
        return True
    DEDUP[key] = True
    if len(DEDUP) > MAX_KEYS:
        DEDUP.popitem(last=False)
    return False

def to_float(x):
    try:
        return float(x)
    except Exception:
        return None

def to_iso8601(ts_str: str) -> str:
    # очікуємо мілісекунди від епохи; якщо ні — даємо поточний UTC
    try:
        ms = int(float(ts_str))
        return datetime.fromtimestamp(ms/1000, tz=timezone.utc).isoformat().replace("+00:00","Z")
    except Exception:
        return datetime.now(timezone.utc).isoformat().replace("+00:00","Z")

def validate(d: dict):
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
    # числа
    entry = to_float(d["entry"]); tp = to_float(d["tp"]); sl = to_float(d["sl"])
    if entry is None or tp is None or sl is None:
        return False, "entry/tp/sl must be numeric"
    return True, {"entry":entry, "tp":tp, "sl":sl}

@app.route("/", methods=["GET"])
def root():
    return "Bot is live", 200

@app.route("/webhook", methods=["POST"])
def webhook():
    try:
        data = request.get_json(force=True, silent=False)
    except Exception as e:
        return jsonify({"status":"error","msg":f"bad json: {e}"}), 400

    ok, info = validate(data or {})
    if not ok:
        return jsonify({"status":"error","msg":info}), 400

    symbol  = str(data["symbol"])
    time_s  = str(data["time"])          # як прийшло з TV (зазвичай мс)
    side    = str(data["side"]).lower()
    pattern = str(data["pattern"]).lower()
    key = f"{symbol}|{time_s}|{side}|{pattern}".lower()

    if seen(key):
        return jsonify({"status":"ok","msg":"duplicate ignored"}), 200

    time_iso = to_iso8601(time_s)
    row = [time_s, time_iso, symbol, pattern, side, info["entry"], info["tp"], info["sl"]]

    newfile = not os.path.exists(LOG_PATH)
    with open(LOG_PATH, "a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        if newfile:
            w.writerow(["time_raw","time_iso","symbol","pattern","side","entry","tp","sl"])
        w.writerow(row)

    return jsonify({"status":"ok","msg":"logged"}), 200

# gunicorn запускає app:app (див. Procfile)
