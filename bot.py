from flask import Flask, request, jsonify
import csv
import os
from datetime import datetime

app = Flask(__name__)
LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)

@app.route('/', methods=['GET'])
def home():
    return "Bot is running!"

@app.route('/webhook', methods=['POST'])
def webhook():
    data = request.json
    if not data or 'signal' not in data or 'p' not in data:
        return jsonify({'status': 'error', 'message': 'Invalid data'}), 400

    signal = data.get("signal")
    symbol = data.get("symbol")
    pattern = data.get("pattern")
    side = data.get("side")
    p = data.get("p")
    timestamp = datetime.utcnow().isoformat()

    filename = f"{pattern}_{side}.csv"
    filepath = os.path.join(LOG_DIR, filename)

    file_exists = os.path.isfile(filepath)
    with open(filepath, mode='a', newline='') as file:
        writer = csv.writer(file)
        if not file_exists:
            writer.writerow(["timestamp", "symbol", "pattern", "side", "p"])
        writer.writerow([timestamp, symbol, pattern, side, p])

    return jsonify({'status': 'success'}), 200

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=10000)