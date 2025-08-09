from flask import Flask, request, jsonify
import csv
import os
from datetime import datetime, timezone

app = Flask(__name__)

LOG_DIR = "logs"
LOG_FILE = os.environ.get("LOG_FILE", "engulfing.csv")
LOG_PATH = os.path.join(LOG_DIR, LOG_FILE)

os.makedirs(LOG_DIR, exist_ok=True)

def to_iso8601(ts_str: str) -> str:
    """Конвертує мс від епохи (або інший формат) у ISO 8601 UTC."""
    try:
        ms = int(float(ts_str))
        return datetime.fromtimestamp(ms/1000, tz=timezone.utc).isoformat().replace("+00:00","Z")
    except Exception:
        return datetime.now(timezone.utc).isoformat().replace("+00:00","Z")

@app.route('/webhook', methods=['POST'])
def webhook():
    try:
        data = request.get_json(force=True)
    except Exception as e:
        return jsonify({"status": "error", "error": str(e)}), 400

    required_fields = ["signal", "symbol", "time", "side", "pattern", "entry", "tp", "sl"]
    for field in required_fields:
        if field not in data:
            return jsonify({"status": "error", "error": f"Missing field: {field}"}), 400

    time_raw = data["time"]
    time_iso = to_iso8601(time_raw)
    symbol = data["symbol"]
    pattern = data["pattern"]
    side = data["side"]
    entry = data["entry"]
    tp = data["tp"]
    sl = data["sl"]

    file_exists = os.path.isfile(LOG_PATH)

    with open(LOG_PATH, mode='a', newline='') as f:
        w = csv.writer(f)
        if not file_exists:
            w.writerow(["time_raw", "time_iso", "symbol", "pattern", "side", "entry", "tp", "sl"])
        w.writerow([time_raw, time_iso, symbol, pattern, side, entry, tp, sl])

    return jsonify({"status": "success", "data": data}), 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
