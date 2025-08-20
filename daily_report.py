#!/usr/bin/env python3
"""
daily_report.py

Генерує ЩОДЕННИЙ файл зі зведенням реальної торгівлі на основі:
1) сигналів з індикатора (CSV з логів бота)
2) фактичних виконань біржі (CSV/JSONL з бота)

Вихід: daily_trades_YYYY-MM-DD.csv (у вказаній папці)

Поля у вихідному файлі:
- signal_id
- signal_time (ISO, Europe/Kyiv)
- delay_ms (мілісекунди від сигналу до відкриття)
- indicator_entry, indicator_sl, indicator_tp
- order_open_time, executed_entry_price, entry_notional
- order_close_time, executed_close_price
- pnl (USDT)
- commission_total (USDT)

Якщо угода ще не закрита — рядок не виводиться (можна додати включення відкритих згодом).
"""

import argparse
import glob
import os
from datetime import datetime
from zoneinfo import ZoneInfo
import pandas as pd

KYIV = ZoneInfo("Europe/Kyiv")

def parse_time(x):
    if pd.isna(x) or x == "":
        return pd.NaT
    # Пробуємо ISO / epoch ms / epoch s
    try:
        return pd.to_datetime(x, utc=True).tz_convert(KYIV)
    except Exception:
        try:
            return pd.to_datetime(int(float(x)), unit='ms', utc=True).tz_convert(KYIV)
        except Exception:
            return pd.to_datetime(int(float(x)), unit='s', utc=True).tz_convert(KYIV)

def load_signals(signals_glob):
    files = sorted(glob.glob(signals_glob))
    if not files:
        return pd.DataFrame(columns=[
            "signal_id","signal_time","symbol","side","pattern",
            "indicator_entry","indicator_sl","indicator_tp","amount"
        ])
    frames = []
    for f in files:
        try:
            df = pd.read_csv(f)
        except Exception:
            df = pd.read_csv(f, sep=';')
        # нормалізуємо назви
        lower = {c.lower().strip(): c for c in df.columns}
        rename = {}
        # id
        for k in ("signal_id","id"):
            if k in lower:
                rename[lower[k]] = "signal_id"; break
        # час
        for k in ("emit_ts","time_iso","time"):
            if k in lower:
                rename[lower[k]] = "signal_time"; break
        # інші
        if "symbol" in lower: rename[lower["symbol"]] = "symbol"
        if "side" in lower: rename[lower["side"]] = "side"
        if "pattern" in lower: rename[lower["pattern"]] = "pattern"
        if "entry" in lower: rename[lower["entry"]] = "indicator_entry"
        if "tp" in lower:    rename[lower["tp"]]    = "indicator_tp"
        if "sl" in lower:    rename[lower["sl"]]    = "indicator_sl"
        if "amount" in lower: rename[lower["amount"]] = "amount"

        df = df.rename(columns=rename)
        for col in ["signal_id","signal_time","symbol","side","pattern",
                    "indicator_entry","indicator_sl","indicator_tp","amount"]:
            if col not in df.columns:
                df[col] = pd.NA
        df["signal_time"] = df["signal_time"].apply(parse_time)
        frames.append(df[["signal_id","signal_time","symbol","side","pattern",
                          "indicator_entry","indicator_sl","indicator_tp","amount"]])
    out = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    out = out.dropna(subset=["signal_id","signal_time"])
    return out

def load_execs(execs_path):
    if not os.path.exists(execs_path):
        return pd.DataFrame(columns=[
            "signal_id","event","time","price","qty","commission","commission_asset","realized_pnl",
            "symbol","side","order_id"
        ])
    if execs_path.endswith(".jsonl"):
        import json
        rows = []
        with open(execs_path, "r", encoding="utf-8") as f:
            for line in f:
                line=line.strip()
                if not line: continue
                try:
                    rows.append(json.loads(line))
                except Exception:
                    pass
        df = pd.DataFrame(rows)
    else:
        try:
            df = pd.read_csv(execs_path)
        except Exception:
            df = pd.read_csv(execs_path, sep=';')

    lower = {c.lower().strip(): c for c in df.columns}
    rename = {}
    for k in ["signal_id","event","time","price","qty","commission","commission_asset",
              "realized_pnl","symbol","side","order_id"]:
        if k in lower: rename[lower[k]] = k
    df = df.rename(columns=rename)
    for col in ["signal_id","event","time","price","qty","commission","commission_asset",
                "realized_pnl","symbol","side","order_id"]:
        if col not in df.columns:
            df[col] = pd.NA

    df["time"] = df["time"].apply(parse_time)
    for col in ["price","qty","commission","realized_pnl"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df["event"] = df["event"].astype(str).str.upper()
    return df

def build_daily(signals_df, execs_df, day):
    """Збираємо завершені угоди за день (за датою signal_time у Europe/Kyiv)."""
    empty_cols = [
        "date","signal_id","signal_time","delay_ms",
        "indicator_entry","indicator_sl","indicator_tp",
        "order_open_time","executed_entry_price","entry_notional",
        "order_close_time","executed_close_price","pnl","commission_total",
        "symbol","side","pattern"
    ]
    if signals_df.empty:
        return pd.DataFrame(columns=empty_cols)

    day_start = pd.Timestamp(day, tz=KYIV)
    day_end = day_start + pd.Timedelta(days=1)
    sday = signals_df[(signals_df["signal_time"]>=day_start) & (signals_df["signal_time"]<day_end)].copy()
    if sday.empty:
        return pd.DataFrame(columns=empty_cols)

    # виконання тільки для потрібних signal_id
    execs = execs_df[execs_df["signal_id"].isin(sday["signal_id"].unique())].copy()

    # агрегуємо OPEN
    opens = execs[execs["event"]=="OPEN"].groupby("signal_id").agg(
        order_open_time=("time","min"),
        executed_entry_price=("price", lambda x: (x*execs.loc[x.index,"qty"]).sum() /
                              max(execs.loc[x.index,"qty"].sum(),1e-9)),
        entry_notional=("price", lambda x: (x*execs.loc[x.index,"qty"]).sum()),
        commission_open=("commission","sum")
    ).reset_index()

    # агрегуємо CLOSE
    closes = execs[execs["event"]=="CLOSE"].groupby("signal_id").agg(
        order_close_time=("time","max"),
        executed_close_price=("price", lambda x: (x*execs.loc[x.index,"qty"]).sum() /
                              max(execs.loc[x.index,"qty"].sum(),1e-9)),
        commission_close=("commission","sum"),
        pnl=("realized_pnl","sum")
    ).reset_index()

    agg = sday.merge(opens, on="signal_id", how="left").merge(closes, on="signal_id", how="left")

    # метрики
    agg["delay_ms"] = (agg["order_open_time"] - agg["signal_time"]).dt.total_seconds()*1000.0
    agg["commission_total"] = agg[["commission_open","commission_close"]].sum(axis=1, skipna=True)

    # тільки повністю закриті
    done = agg.dropna(subset=["order_open_time","order_close_time",
                              "executed_entry_price","executed_close_price"]).copy()
    done.insert(0, "date", day)

    cols = [
        "date","signal_id","signal_time","delay_ms",
        "indicator_entry","indicator_sl","indicator_tp",
        "order_open_time","executed_entry_price","entry_notional",
        "order_close_time","executed_close_price","pnl","commission_total",
        "symbol","side","pattern"
    ]
    return done[cols].sort_values("signal_time")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--signals", default="logs/*.csv",
                    help="Глоб-шаблон для CSV із сигналами (за замовчуванням logs/*.csv)")
    ap.add_argument("--execs", default="logs/executions.csv",
                    help="Файл з виконаннями біржі (CSV або JSONL)")
    ap.add_argument("--date", default=None,
                    help="Дата у форматі YYYY-MM-DD (Europe/Kyiv). Якщо не задано — поточна дата.")
    ap.add_argument("--outdir", default=".",
                    help="Куди писати daily-файл")
    args = ap.parse_args()

    day = args.date or datetime.now(KYIV).strftime("%Y-%m-%d")
    os.makedirs(args.outdir, exist_ok=True)

    signals = load_signals(args.signals)
    execs = load_execs(args.execs)

    daily = build_daily(signals, execs, day)
    out_path = os.path.join(args.outdir, f"daily_trades_{day}.csv")
    daily.to_csv(out_path, index=False)
    print(f"Written: {out_path} ({len(daily)} rows)")

if __name__ == "__main__":
    main()
