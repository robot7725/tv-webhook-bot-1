import os, csv, hmac, hashlib, json
from collections import OrderedDict
from datetime import datetime, timezone
from flask import Flask, request, jsonify

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

os.makedirs(LOG_DIR, exist_ok=True)
CSV_PATH  = os.path.join(LOG_DIR, LOG_FILE)
TECH_PATH = os.path.join(LOG_DIR, TECH_LOG)

# ===== utils =====
DEDUP = OrderedDict()

def techlog(entry: dict):
    entry["ts"] = datetime.now(timezone.utc).isoformat().replace("+00:00","Z")
    with open(TECH_PATH, "a", encoding="utf-8") as f:
        f.write(json.dumps(entry, ensure_ascii=False) + "\n")

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
    try:
        ms = int(float(ts_str))
        return datetime.fromtimestamp(ms/1000, tz=timezone.utc).isoformat().replace("+00:00","Z")
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
    return f"{pattern}|{side}|{time_raw}".lower()

def dedup_seen(key: str) -> bool:
    if key in DEDUP:
        DEDUP.move_to_end(key)
        return True
    DEDUP[key] = True
    if len(DEDUP) > MAX_KEYS:
        DEDUP.popitem(last=False)
    return False

# ===== routes =====
@app.route("/", methods=["GET"])
def root():
    return "Bot is live", 200

@app.route("/healthz", methods=["GET"])
def healthz():
    return jsonify({
        "status":"ok",
        "version":"1.0.0",
        "env":ENV_MODE,
        "time": datetime.now(timezone.utc).isoformat().replace("+00:00","Z")
    }), 200

@app.route("/webhook", methods=["POST"])
def webhook():
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

    symbol  = str(data["symbol"])
    time_s  = str(data["time"])
    side    = str(data["side"]).lower()
    pattern = str(data["pattern"]).lower()
    sig_id  = build_id(pattern, side, time_s)

    if dedup_seen(sig_id):
        techlog({"level":"info","msg":"duplicate_ignored","id":sig_id})
        return jsonify({"status":"ok","msg":"ignored","id":sig_id}), 200

    rotate_if_needed(CSV_PATH)

    time_iso = to_iso8601(time_s)
    row = [time_s, time_iso, symbol, pattern, side, info["entry"], info["tp"], info["sl"], sig_id]
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

    techlog({"level":"info","msg":"logged","id":sig_id,"symbol":symbol,"side":side})
    return jsonify({"status":"ok","msg":"logged","id":sig_id}), 200
