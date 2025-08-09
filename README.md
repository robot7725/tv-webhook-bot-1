# TradingView Webhook Bot (Engulfing)

POST /webhook — приймає JSON від Pine alert() у форматі:
{"signal":"entry","symbol":"XXX","time":"<ms>","side":"long|short","pattern":"engulfing","entry":"...","tp":"...","sl":"..."}

Логи:
- CSV: logs/<LOG_FILE> (за замовч. engulfing.csv)
- Tech JSONL: logs/<TECH_LOG> (за замовч. tech.jsonl)

ENV:
LOG_DIR=logs
LOG_FILE=engulfing.csv
TECH_LOG=tech.jsonl
ALLOW_PATTERN=engulfing
ENV=prod
DEDUP_CACHE=2000
ROTATE_BYTES=5242880
KEEP_FILES=3
WEBHOOK_SECRET= (порожньо = підпис вимкнено)

Health:
GET /         -> "Bot is live"
GET /healthz  -> JSON {"status":"ok",...}
