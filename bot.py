from flask import Flask, request, jsonify
import os
import csv
from datetime import datetime

app = Flask(__name__)

LOGS_DIR = "logs"
os.makedirs(LOGS_DIR, exist_ok=True)

REQUIRED_FIELDS = {"signal", "symbol", "time", "side", "pattern", "entry", "tp", "sl", "p", "leverage"}
ALLOWED_PATTERNS = {"engulfing", "wick", "insidebar", "threebar"}

def is_duplicate(filename, time_str, symbol, side):
    if not os.path.exists(filename):
        return False
    with open(filename, newline="") as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            if row["time"] == time_str and row["symbol"] == symbol and row["side"] == side:
                return True
    return False

@app.route("/webhook", methods=["POST"])
def webhook():
    data = request.get_json()

    if not data or not REQUIRED_FIELDS.issubset(data.keys()):
        return jsonify({"status": "error", "message": "Missing required fields"}), 400

    try:
        signal     = data["signal"]
        symbol     = data["symbol"]
        time_str   = data["time"]
        side       = data["side"]
        pattern    = data["pattern"].lower()
        if pattern not in ALLOWED_PATTERNS:
            return jsonify({"status": "error", "message": "Invalid pattern"}), 400

        entry      = float(data["entry"])
        tp         = float(data["tp"])
        sl         = float(data["sl"])
        p          = float(data["p"])
        leverage   = int(data["leverage"])

        filename = os.path.join(LOGS_DIR, f"{pattern}.csv")
        timestamp = datetime.fromisoformat(time_str.replace("Z", "+00:00"))
        iso_time = timestamp.isoformat()

        if is_duplicate(filename, iso_time, symbol, side):
            print(f"[DUPLICATE] {symbol} {side} at {iso_time}")
            return jsonify({"status": "duplicate"}), 200

        file_exists = os.path.isfile(filename)
        with open(filename, "a", newline="") as csvfile:
            writer = csv.writer(csvfile)
            if not file_exists:
                writer.writerow(["time", "symbol", "side", "entry", "tp", "sl", "p", "leverage"])
            writer.writerow([iso_time, symbol, side, entry, tp, sl, p, leverage])

        print(f"[RECEIVED] {pattern.upper()} {side} on {symbol} at {iso_time}")
        return jsonify({"status": "ok"}), 200

    except Exception as e:
        print(f"[ERROR] {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500

if __name__ == "__main__":
    app.run(port=5000)
