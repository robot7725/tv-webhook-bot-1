from flask import Flask, request, jsonify
import os, csv

app = Flask(__name__)
LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)

@app.route("/webhook", methods=["POST"])
def webhook():
    data = request.get_json(silent=True)
    if not isinstance(data, dict):
        return jsonify({"status": "error", "message": "Invalid JSON"}), 400

    # Обов'язкові для всіх етапів
    for k in ["signal", "pattern", "side", "entry"]:
        if k not in data:
            return jsonify({"status": "error", "message": f"Missing required field: {k}"}), 400

    if str(data["signal"]).lower() != "entry":
        return jsonify({"status": "ignored", "message": "Not an entry signal"}), 200

    # Нормалізація
    symbol  = str(data.get("symbol", ""))
    time_   = str(data.get("time", ""))
    pattern = str(data["pattern"]).lower()
    side    = str(data["side"]).lower()
    entry   = str(data["entry"])
    sl      = str(data.get("sl", ""))          # може бути відсутнім на Stage 1
    tp      = str(data.get("tp", ""))          # опційний на Stage 3

    # Визначимо stage для логів
    stage = "1"
    if sl and not tp:
        stage = "2"
    if sl and tp:
        stage = "3"

    # Пишемо окремий CSV по кожній комбінації pattern+side
    filename = f"{pattern}_{side}.csv"
    filepath = os.path.join(LOG_DIR, filename)
    new_file = not os.path.exists(filepath)

    with open(filepath, "a", newline="") as f:
        w = csv.writer(f)
        if new_file:
            w.writerow(["time","symbol","pattern","side","stage","entry","sl","tp"])
        w.writerow([time_, symbol, pattern, side, stage, entry, sl, tp])

    return jsonify({"status": "success", "stage": stage, "saved": {"pattern": pattern, "side": side}}), 200

if __name__ == "__main__":
    # Render/локально
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
