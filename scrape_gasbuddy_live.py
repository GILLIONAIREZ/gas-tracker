"""
Capture the GasBuddy live ticking national average.

Designed to run every 30 minutes via a separate GitHub Actions workflow.
Each run appends one row to data/gasbuddy_intraday.csv with the live tick
plus the pre-computed comparison anchors GasBuddy returns.

Skips writes when LastUpdatedTime hasn't advanced since the previous capture
(no point recording stale ticks as if they were new observations).

Endpoint: GET https://fuelinsights.gasbuddy.com/api/LiveAvg/?id=500000&countryID=500000
"""
import csv
import datetime as dt
import json
import sys
from pathlib import Path

import requests

ENDPOINT = "https://fuelinsights.gasbuddy.com/api/LiveAvg/"
PARAMS = {"id": "500000", "countryID": "500000"}
OUTPUT = Path("data/gasbuddy_intraday.csv")
SNAPSHOT_DIR = Path("data/gasbuddy_snapshots")

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Referer": "https://fuelinsights.gasbuddy.com/",
}

CSV_COLUMNS = [
    "scraped_at_utc",        # when WE fetched
    "last_updated_at",       # when GASBUDDY last recomputed (their timestamp)
    "live_ticking_avg",      # the headline live national average
    "yesterday_final_avg",   # locked-in value for prior day, useful for delta calc
    "one_week_ago_avg",
    "one_month_ago_avg",
    "one_year_ago_avg",
    "live_vs_yesterday",     # live_ticking_avg - yesterday_final_avg, the gap-closing signal
    "today_trend",           # priceTrend enum from Today field
]


def fetch():
    r = requests.get(ENDPOINT, params=PARAMS, headers=HEADERS, timeout=30)
    r.raise_for_status()
    return r.json()


def extract(data):
    """Pull the fields we care about into a flat dict."""
    today = data["AvgPriceDict"]["Today"]
    yday = data["AvgPriceDict"]["OneDayAgo"]
    week = data["AvgPriceDict"]["OneWeekAgo"]
    month = data["AvgPriceDict"]["OneMonthAgo"]
    year = data["AvgPriceDict"]["OneYearAgo"]

    live = float(data["LiveTickingAvg"])
    yesterday_final = float(yday["AvgPrice"])

    return {
        "scraped_at_utc": dt.datetime.utcnow().isoformat(timespec="seconds"),
        "last_updated_at": data.get("LastUpdatedTime", ""),
        "live_ticking_avg": live,
        "yesterday_final_avg": yesterday_final,
        "one_week_ago_avg": float(week["AvgPrice"]),
        "one_month_ago_avg": float(month["AvgPrice"]),
        "one_year_ago_avg": float(year["AvgPrice"]),
        "live_vs_yesterday": round(live - yesterday_final, 4),
        "today_trend": today.get("priceTrend"),
    }


def last_recorded_update_time():
    """Read the most recent last_updated_at from our CSV. None if file empty."""
    if not OUTPUT.exists():
        return None
    with OUTPUT.open() as f:
        rows = list(csv.DictReader(f))
    if not rows:
        return None
    return rows[-1].get("last_updated_at") or None


def append_row(row):
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    new_file = not OUTPUT.exists()
    with OUTPUT.open("a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
        if new_file:
            w.writeheader()
        w.writerow(row)


def archive_snapshot(raw_data):
    """Save raw response so we have point-in-time data for clean future backtests."""
    SNAPSHOT_DIR.mkdir(parents=True, exist_ok=True)
    ts = dt.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    (SNAPSHOT_DIR / f"{ts}.json").write_text(json.dumps(raw_data, indent=2))


def sanity_check(row):
    p = row["live_ticking_avg"]
    if not (0.5 < p < 15.0):
        raise ValueError(f"live_ticking_avg out of range: {p}")
    if not row["last_updated_at"]:
        raise ValueError("Missing LastUpdatedTime")


def main():
    raw = fetch()
    row = extract(raw)
    sanity_check(row)

    prev = last_recorded_update_time()
    if prev and prev == row["last_updated_at"]:
        print(f"GasBuddy hasn't recomputed since {prev}; skipping write")
        return

    archive_snapshot(raw)
    append_row(row)

    print(f"Recorded tick @ {row['last_updated_at']}")
    print(f"  Live: {row['live_ticking_avg']:.3f}")
    print(f"  vs yesterday final: {row['live_vs_yesterday']:+.3f}")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"FATAL: {e}", file=sys.stderr)
        sys.exit(1)
