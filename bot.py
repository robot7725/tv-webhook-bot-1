from flask import Flask, request, jsonify
import os
import csv

app = Flask(__name__)
LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)

@app.route("/webhook", methods=["POST"])
def webhook():
    data = request.get_json()

    required_fields = ["signal", "pattern", "side", "entry"]
    if not all(field in data for field in required_fields):
        return jsonify({"status": "error", "message": "Missing required fields"}), 400

    if data["signal"] != "entry":
        return jsonify({"status": "ignored", "message": "Not an entry signal"}), 200

    pattern = data["pattern"].lower()
    side = data["side"].lower()
    entry = data["entry"]

    filename = f"{pattern}_{side}.csv"
    filepath = os.path.join(LOG_DIR, filename)
    is_new_file = not os.path.exists(filepath)

    with open(filepath, mode="a", newline="") as file:
        writer = csv.writer(file)
        if is_new_file:
            writer.writerow(["entry"])
        writer.writerow([entry])

    return jsonify({"status": "success", "message": "Signal saved"}), 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
