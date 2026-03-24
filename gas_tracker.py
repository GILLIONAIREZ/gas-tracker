#!/usr/bin/env python3
"""
US Daily Gas Price Tracker
Fetches national average gas price from AAA, emails a summary,
and saves data to CSV for trend tracking.

Designed to run in GitHub Actions. Secrets come from environment variables.
"""

import requests
from bs4 import BeautifulSoup
import csv
import os
import smtplib
import json
import re
import sys
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime

# ── Config (from environment variables) ──────────────────────────────────────
EMAIL_FROM   = os.environ.get("EMAIL_FROM", "")
EMAIL_TO     = os.environ.get("EMAIL_TO", "")
APP_PASSWORD = os.environ.get("GMAIL_APP_PASSWORD", "")

BASE_DIR     = os.path.dirname(os.path.abspath(__file__))
DATA_DIR     = os.path.join(BASE_DIR, "data")
DATA_FILE    = os.path.join(DATA_DIR, "gas_data.csv")
STATE_FILE   = os.path.join(DATA_DIR, "state.json")

AAA_URL      = "https://gasprices.aaa.com/"
HEADERS      = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
}

# ── Ensure data dir exists ───────────────────────────────────────────────────
os.makedirs(DATA_DIR, exist_ok=True)


# ── Scraping ─────────────────────────────────────────────────────────────────
def fetch_prices():
    """
    Fetch gas price data from AAA.
    Returns dict: current, yesterday, week_ago, month_ago, year_ago, price_date.
    """
    resp = requests.get(AAA_URL, headers=HEADERS, timeout=20)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")

    data = {}

    # Strategy 1: table.table-mob
    # AAA layout: rows = time periods, cols = fuel grades.
    # Regular Unleaded is column index 1.
    table = soup.find("table", class_="table-mob")
    if table:
        row_map = {
            "current":   "current avg.",
            "yesterday": "yesterday avg.",
            "week_ago":  "week ago avg.",
            "month_ago": "month ago avg.",
            "year_ago":  "year ago avg.",
        }
        for row in table.find_all("tr"):
            cells = [c.get_text(strip=True) for c in row.find_all(["td", "th"])]
            if len(cells) < 2:
                continue
            label = cells[0].lower()
            for key, expected in row_map.items():
                if expected in label:
                    data[key] = cells[1]
                    break

    # Strategy 2: regex fallback
    if not data.get("current"):
        text = soup.get_text(" ", strip=True)
        prices = re.findall(r'\$\s*(\d+\.\d{3})', text)
        if prices:
            data["current"] = f"${prices[0]}"
            if len(prices) > 1:
                data["yesterday"] = f"${prices[1]}"

    # "Price as of" date
    text = soup.get_text(" ", strip=True)
    m = re.search(r'[Pp]rice\s+as\s+of\s+(\d{1,2}/\d{1,2}/\d{2,4})', text)
    if m:
        data["price_date"] = m.group(1)
    else:
        data["price_date"] = datetime.utcnow().strftime("%m/%d/%y")

    return data


# ── Helpers ──────────────────────────────────────────────────────────────────
def to_float(price_str):
    if not price_str:
        return None
    try:
        return float(re.sub(r'[^\d.]', '', price_str))
    except ValueError:
        return None


def fmt_change(current, reference):
    if current is None or reference is None:
        return "N/A"
    diff = current - reference
    if diff > 0:
        return f"▲ +${diff:.3f}"
    elif diff < 0:
        return f"▼ -${abs(diff):.3f}"
    return "→ $0.000"


def load_state():
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE) as f:
            return json.load(f)
    return {}


def save_state(state):
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=2)


def append_csv(filepath, row, header=None):
    new_file = not os.path.exists(filepath)
    with open(filepath, "a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        if new_file and header:
            w.writerow(header)
        w.writerow(row)


# ── Email ────────────────────────────────────────────────────────────────────
DASHBOARD_URL = os.environ.get("DASHBOARD_URL", "")


def build_email(price_data):
    cur  = to_float(price_data.get("current"))
    yest = to_float(price_data.get("yesterday"))
    wk   = to_float(price_data.get("week_ago"))
    mo   = to_float(price_data.get("month_ago"))
    yr   = to_float(price_data.get("year_ago"))
    pd   = price_data.get("price_date", "today")

    day_chg = fmt_change(cur, yest)
    wk_chg  = fmt_change(cur, wk)
    mo_chg  = fmt_change(cur, mo)
    yr_chg  = fmt_change(cur, yr)

    subject = f"Gas ${cur:.3f}  {day_chg}  ({pd})"

    def pf(v):
        return f"${v:.3f}" if v is not None else "N/A"

    plain = (
        f"US National Average Gas Price — {pd}\n"
        f"{'─' * 38}\n"
        f"Today       {pf(cur)}\n"
        f"Yesterday   {pf(yest)}   {day_chg}\n"
        f"1 Week Ago  {pf(wk)}   {wk_chg}\n"
        f"1 Month Ago {pf(mo)}   {mo_chg}\n"
        f"1 Year Ago  {pf(yr)}   {yr_chg}\n"
        f"\nSource: gasprices.aaa.com  |  Regular Unleaded\n"
    )
    if DASHBOARD_URL:
        plain += f"\nView trends: {DASHBOARD_URL}\n"

    def color(cur_val, ref_val):
        if cur_val is None or ref_val is None:
            return "#888"
        return "#c0392b" if cur_val > ref_val else "#27ae60"

    rows_html = ""
    for label, ref, chg in [
        ("Yesterday",   yest, day_chg),
        ("1 Week Ago",  wk,   wk_chg),
        ("1 Month Ago", mo,   mo_chg),
        ("1 Year Ago",  yr,   yr_chg),
    ]:
        bg = "#f9f9f9" if label in ("Yesterday", "1 Month Ago") else "#ffffff"
        c  = color(cur, ref)
        rows_html += (
            f'<tr style="background:{bg};">'
            f'<td style="padding:8px 14px;color:#555;">{label}</td>'
            f'<td style="padding:8px 14px;">{pf(ref)}</td>'
            f'<td style="padding:8px 14px;font-weight:bold;color:{c};">{chg}</td>'
            f'</tr>'
        )

    dashboard_link = ""
    if DASHBOARD_URL:
        dashboard_link = (
            f'<p style="margin-top:12px;text-align:center;">'
            f'<a href="{DASHBOARD_URL}" style="color:#2980b9;font-size:14px;font-weight:bold;">'
            f'📊 View Full Trend Dashboard</a></p>'
        )

    html = f"""
<html><body style="font-family:Arial,sans-serif;max-width:480px;margin:0 auto;color:#222;">
  <h2 style="margin-bottom:4px;">⛽ US Gas Price — {pd}</h2>
  <p style="color:#888;margin-top:0;font-size:13px;">Regular Unleaded · National Average</p>
  <table style="width:100%;border-collapse:collapse;margin-top:16px;">
    <tr style="background:#222;color:#fff;">
      <td style="padding:12px 14px;font-size:16px;font-weight:bold;" colspan="2">Today</td>
      <td style="padding:12px 14px;font-size:22px;font-weight:bold;">{pf(cur)}</td>
    </tr>
    {rows_html}
  </table>
  {dashboard_link}
  <p style="margin-top:16px;font-size:11px;color:#aaa;">
    Source: <a href="https://gasprices.aaa.com" style="color:#aaa;">AAA Gas Prices</a>
  </p>
</body></html>
"""
    return subject, plain, html


def send_email(subject, plain, html=None):
    if not EMAIL_FROM or not APP_PASSWORD:
        print("⚠️  Email credentials not set — skipping email.")
        return

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"]    = EMAIL_FROM
    msg["To"]      = EMAIL_TO
    msg.attach(MIMEText(plain, "plain"))
    if html:
        msg.attach(MIMEText(html, "html"))

    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as srv:
        srv.login(EMAIL_FROM, APP_PASSWORD)
        srv.sendmail(EMAIL_FROM, EMAIL_TO, msg.as_string())
    print(f"✅ Email sent: {subject}")


# ── Main ─────────────────────────────────────────────────────────────────────
def main():
    now = datetime.utcnow()
    fetch_time = now.strftime("%Y-%m-%d %H:%M:%S")
    print(f"🔄 Fetching AAA gas prices at {fetch_time} UTC ...")

    # 1. Fetch
    try:
        price_data = fetch_prices()
        print(f"   Fetched: {price_data}")
    except Exception as e:
        print(f"❌ Fetch failed: {e}")
        try:
            send_email(
                "⚠️ Gas Tracker fetch error",
                f"Could not retrieve price at {fetch_time} UTC.\n\nError: {e}",
            )
        except Exception:
            pass
        sys.exit(1)

    current_price = price_data.get("current", "")
    price_date    = price_data.get("price_date", "")

    # 2. Check if we already have this day's price
    state  = load_state()
    is_new = price_date != state.get("last_price_date", "")

    if not is_new:
        print(f"ℹ️  Already have {price_date} ({current_price}). No action.")
        return

    print(f"🆕 New price for {price_date}: {current_price}")

    # 3. Save to CSV
    append_csv(
        DATA_FILE,
        [
            fetch_time,
            price_date,
            price_data.get("current", ""),
            price_data.get("yesterday", ""),
            price_data.get("week_ago", ""),
            price_data.get("month_ago", ""),
            price_data.get("year_ago", ""),
        ],
        header=[
            "fetch_time", "price_date",
            "current", "yesterday",
            "week_ago", "month_ago", "year_ago",
        ],
    )

    # 4. Send email
    try:
        subject, plain, html = build_email(price_data)
        send_email(subject, plain, html)
    except Exception as e:
        print(f"❌ Email failed: {e}")

    # 5. Update state
    state["last_price_date"]  = price_date
    state["last_price"]       = current_price
    state["last_notify_time"] = fetch_time
    save_state(state)
    print("✅ Done.")


if __name__ == "__main__":
    main()
