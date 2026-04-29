"""
Fetch the full 5-year GasBuddy national average history and write to CSV.

Designed to be run daily in GitHub Actions:
- Refetches the full 5Y series every run (cheap, ~1800 rows)
- Idempotent: dedupes on date, last-write-wins so revisions overwrite
- Sanity-checks the response so a broken endpoint fails the workflow loudly

Endpoint: POST https://fuelinsights.gasbuddy.com/api/HighChart/GetHighChartRecords/
Discovered via DevTools network capture on https://fuelinsights.gasbuddy.com/charts
"""
import csv
import datetime as dt
import sys
from pathlib import Path

import requests

ENDPOINT = "https://fuelinsights.gasbuddy.com/api/HighChart/GetHighChartRecords/"
OUTPUT = Path("data/gasbuddy_data.csv")

# Match what the browser sends. fuelType=3=regular, timeWindow=13=5Y, frequency=1=daily.
PAYLOAD = {
    "regionID": ["500000"],   # 500000 = US national
    "fuelType": 3,
    "timeWindow": [13],
    "frequency": 1,
}

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Content-Type": "application/json;charset=UTF-8",
    "Origin": "https://fuelinsights.gasbuddy.com",
    "Referer": "https://fuelinsights.gasbuddy.com/charts",
}


def fetch():
    r = requests.post(ENDPOINT, json=PAYLOAD, headers=HEADERS, timeout=30)
    r.raise_for_status()
    return r.json()


def parse(data):
    """Return list of (iso_date, price) tuples."""
    if not isinstance(data, list) or not data:
        raise ValueError(f"Unexpected response shape: {type(data).__name__}")
    rows = []
    for region in data:
        for entry in region.get("USList", []):
            d = dt.datetime.strptime(entry["datetime"], "%m/%d/%Y").date()
            rows.append((d.isoformat(), float(entry["price"])))
    return rows


def sanity_check(rows):
    """Fail loudly if the data looks wrong, so a broken endpoint doesn't silently corrupt the CSV."""
    if len(rows) < 1500:
        raise ValueError(f"Too few rows ({len(rows)}); expected ~1800 for 5Y daily")
    prices = [p for _, p in rows]
    if not all(0.5 < p < 15.0 for p in prices):
        out_of_range = [p for p in prices if not (0.5 < p < 15.0)]
        raise ValueError(f"Prices out of plausible range: {out_of_range[:5]}")
    latest_date = dt.date.fromisoformat(rows[-1][0])
    age = (dt.date.today() - latest_date).days
    if age > 3:
        raise ValueError(f"Latest GasBuddy data is {age} days old (last: {latest_date})")


def merge_with_existing(new_rows):
    """Merge new data with existing CSV, deduping by date (new wins)."""
    by_date = {}
    if OUTPUT.exists():
        with OUTPUT.open() as f:
            reader = csv.DictReader(f)
            for row in reader:
                by_date[row["date"]] = float(row["national_avg_regular"])
    for d, p in new_rows:
        by_date[d] = p  # new value wins on conflict (handles revisions)
    return sorted(by_date.items())


def write_csv(rows):
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    with OUTPUT.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["date", "national_avg_regular"])
        for d, p in rows:
            w.writerow([d, f"{p:.3f}"])


def main():
    data = fetch()
    rows = parse(data)
    sanity_check(rows)
    merged = merge_with_existing(rows)
    write_csv(merged)
    print(f"Wrote {len(merged)} rows to {OUTPUT}")
    print(f"Range: {merged[0][0]} -> {merged[-1][0]}")
    print(f"Latest: {merged[-1][1]:.3f}")
    if len(merged) >= 2:
        delta = merged[-1][1] - merged[-2][1]
        print(f"Day-over-day: {delta:+.3f}")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"FATAL: {e}", file=sys.stderr)
        sys.exit(1)
