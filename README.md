POST /webhook — приймає JSON від TradingView alert() у форматі одним рядком:
{"signal":"entry","symbol":"XXX","time":"","side":"long|short","pattern":"<ALLOW_PATTERN>","entry":"<num>","tp":"<num>","sl":"<num>"}

signal — завжди "entry"

symbol — тикер (наприклад, BTCUSDT)

time — мітка часу з TradingView ({{time}}, epoch ms)

side — long або short

pattern — має збігатися з ALLOW_PATTERN у ENV (engulfing або inside)

entry, tp, sl — числові значення
