# send_mail.py
import os, ssl, smtplib, argparse
from email.message import EmailMessage
from pathlib import Path

SMTP_HOST  = os.environ.get("SMTP_HOST", "smtp.gmail.com")
SMTP_PORT  = int(os.environ.get("SMTP_PORT", "465"))
SMTP_USER  = os.environ.get("SMTP_USER", "")
SMTP_PASS  = os.environ.get("SMTP_PASS", "")
EMAIL_FROM = os.environ.get("EMAIL_FROM", SMTP_USER)
EMAIL_TO   = os.environ.get("EMAIL_TO", "")

def send_file(path: Path, subject: str, body: str = ""):
    if not EMAIL_TO:
        raise SystemExit("EMAIL_TO не задано")
    msg = EmailMessage()
    msg["From"] = EMAIL_FROM
    msg["To"] = EMAIL_TO
    msg["Subject"] = subject
    msg.set_content(body or f"Надсилаю звіт: {path.name}")
    if path and path.exists():
        msg.add_attachment(path.read_bytes(), maintype="text", subtype="csv", filename=path.name)
    ctx = ssl.create_default_context()
    with smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT, context=ctx) as s:
        if SMTP_USER and SMTP_PASS:
            s.login(SMTP_USER, SMTP_PASS)
        s.send_message(msg)

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--file", required=True, help="Шлях до CSV")
    ap.add_argument("--subject", default="Daily CSV report")
    args = ap.parse_args()
    p = Path(args.file)
    if not p.exists():
        raise SystemExit(f"Файл не знайдено: {p}")
    send_file(p, args.subject)
    print("[OK] Лист відправлено:", p)
