from flask import Flask, request, jsonify
import os, csv, time

app = Flask(__name__)
LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)

# Примітивна дедуплікація у пам'яті (ключ на 30с)
SEEN = {}  # key -> timestamp
DEDUP_TTL = 30

def dedup_key(d):
    # якщо є унікальний id — краще ним
    if "id" in d:
        return f"id:{d.get('id')}"
    # запасний ключ
    return f"{d.get('pattern','')}|{d.get('side','')}|{d.get('time','')}"

def purge_seen():
    now = time.time()
    for k, t in list(SEEN.items()):
        if now - t > DEDUP_TTL:
            del SEEN[k]

@app.route("/healthz", methods=["GET"])
def health():
    return jsonify({"status":"ok","service":"tv-webhook-bot","time":time.time()}), 200

@app.route("/webhook", methods=["POST"])
def webhook():
    data = request.get_json(silent=True)
    if not isinstance(data, dict):
        return jsonify({"status": "error", "message": "Invalid JSON"}), 400

    # Базові поля для всіх етапів
    for k in ["signal", "pattern", "side", "entry"]:
        if k not in data:
            return jsonify({"status": "error", "message": f"Missing required field: {k}"}), 400
    if str(data["signal"]).lower() != "entry":
        return jsonify({"status": "ignored", "message": "Not an entry signal"}), 200

    # Антидубль
    purge_seen()
    key = dedup_key(data)
    if key in SEEN:
        return jsonify({"status":"ignored","reason":"duplicate","key":key}), 200
    SEEN[key] = time.time()

    # Нормалізація
    symbol  = str(data.get("symbol",""))
    time_   = str(data.get("time",""))
    pattern = str(data["pattern"]).lower()
    side    = str(data["side"]).lower()
    entry   = str(data["entry"])
    sl      = str(data.get("sl",""))
    tp      = str(data.get("tp",""))
    uid     = str(data.get("id",""))

    # Визначаємо stage
    stage = "1"
    if sl and not tp: stage = "2"
    if sl and tp:     stage = "3"

    # Логи
    fn = f"{pattern}_{side}.csv"
    fp = os.path.join(LOG_DIR, fn)
    newf = not os.path.exists(fp)
    with open(fp, "a", newline="") as f:
        w = csv.writer(f)
        if newf: w.writerow(["time","symbol","pattern","side","stage","entry","sl","tp","id"])
        w.writerow([time_, symbol, pattern, side, stage, entry, sl, tp, uid])

    return jsonify({"status": "success", "stage": stage}), 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
