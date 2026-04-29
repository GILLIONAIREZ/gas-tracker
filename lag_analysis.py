"""
Unified GasBuddy -> AAA predictive analysis.

Tests every reasonable GasBuddy-derived feature against next-day AAA delta and
ranks them by Pearson correlation. Includes:

  - Daily features (always available from data/gasbuddy_data.csv):
      Day-over-day GasBuddy deltas at lags 0-7
      Level differences between GasBuddy and AAA at lags 0-7

  - Intraday features (computed only when data/gasbuddy_intraday.csv has
    >= MIN_INTRADAY_DAYS days of coverage):
      End-of-day live tick
      Tick at specific hours (noon ET, 3pm, 6pm, 9pm)
      Intraday velocity (slope of ticks during the day)
      Intraday range (max - min during the day)
      Live-vs-yesterday delta at end of day

The script gracefully skips intraday features when the CSV is too sparse,
so it produces useful output from day 1 and gets sharper as data accumulates.

Output:
  - Prints a ranked table of features by |correlation|
  - Writes data/lag_analysis.json for the dashboard
"""
import csv
import datetime as dt
import json
import re
import statistics
from pathlib import Path

# --- Configuration ---------------------------------------------------------

AAA_CSV       = Path("data/gas_data.csv")
AAA_DATE_COL  = "price_date"       # M/D/YY format e.g. "3/23/26"
AAA_PRICE_COL = "current"          # dollar-prefixed e.g. "$3.956"

GB_DAILY_CSV    = Path("data/gasbuddy_data.csv")
GB_INTRADAY_CSV = Path("data/gasbuddy_intraday.csv")
OUTPUT_JSON     = Path("data/lag_analysis.json")

# Minimum days of intraday coverage before intraday features are computed.
MIN_INTRADAY_DAYS = 14

# Hours (US Eastern, 24h) at which to sample the intraday tick.
INTRADAY_SAMPLE_HOURS_ET = [12, 15, 18, 21]


# --- Helpers ---------------------------------------------------------------

def _parse_date(d_str):
    """Normalize a date string to ISO format, handling multiple input formats."""
    d = d_str.strip()
    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%m/%d/%y"):
        try:
            return dt.datetime.strptime(d, fmt).date().isoformat()
        except ValueError:
            continue
    raise ValueError(f"Unrecognised date format: {d!r}")


def _parse_price(p_str):
    """Strip currency symbols / whitespace and return float, or None."""
    clean = re.sub(r"[^\d.]", "", p_str.strip())
    if not clean:
        return None
    try:
        return float(clean)
    except ValueError:
        return None


def load_daily_csv(path, date_col, price_col):
    """Load a daily CSV into a {date_iso: price} dict.

    Handles:
    - ISO dates (YYYY-MM-DD) and M/D/YY or M/D/YYYY formats
    - Dollar-prefixed prices ("$3.956")
    - Multiple rows per date (last non-null wins — handles AAA backfill rows)
    """
    out = {}
    with path.open() as f:
        for row in csv.DictReader(f):
            d_raw = row.get(date_col, "").strip()
            p_raw = row.get(price_col, "").strip()
            if not d_raw or not p_raw:
                continue
            try:
                d = _parse_date(d_raw)
            except ValueError:
                continue
            p = _parse_price(p_raw)
            if p is None:
                continue
            out[d] = p
    return out


def to_deltas(series):
    """Convert {date: price} into {date: day_over_day_change}, only for consecutive dates."""
    items = sorted(series.items())
    deltas = {}
    for i in range(1, len(items)):
        prev_d, prev_p = items[i - 1]
        cur_d, cur_p   = items[i]
        if (dt.date.fromisoformat(cur_d) - dt.date.fromisoformat(prev_d)).days == 1:
            deltas[cur_d] = cur_p - prev_p
    return deltas


def pearson(pairs):
    """Pearson correlation on list of (x, y) tuples. Returns None if undefined."""
    if len(pairs) < 10:
        return None
    xs, ys = zip(*pairs)
    mx, my = statistics.mean(xs), statistics.mean(ys)
    num = sum((x - mx) * (y - my) for x, y in pairs)
    dx  = sum((x - mx) ** 2 for x in xs) ** 0.5
    dy  = sum((y - my) ** 2 for y in ys) ** 0.5
    if dx == 0 or dy == 0:
        return None
    return num / (dx * dy)


def align(feature_by_date, target_by_date, lag_days=1):
    """
    For each date d in feature_by_date, pair feature[d] with target[d + lag_days].
    Default lag=1: today's feature predicts tomorrow's target.
    """
    pairs = []
    for d_str, f_val in feature_by_date.items():
        d = dt.date.fromisoformat(d_str)
        d_target = (d + dt.timedelta(days=lag_days)).isoformat()
        if d_target in target_by_date:
            pairs.append((f_val, target_by_date[d_target]))
    return pairs


# --- Intraday feature extraction ------------------------------------------

def load_intraday_csv(path):
    """
    Load intraday CSV into {date_iso_ET: [(timestamp_et, live_tick), ...]}.
    Groups ticks by ET calendar date so end-of-day features make sense.
    """
    if not path.exists():
        return {}
    by_date = {}
    with path.open() as f:
        for row in csv.DictReader(f):
            ts_utc = dt.datetime.fromisoformat(row["scraped_at_utc"])
            # Rough UTC -> ET offset. Off by one hour during DST transitions,
            # which only affects hour-sampling features at the margin.
            ts_et = ts_utc - dt.timedelta(hours=4)
            d = ts_et.date().isoformat()
            tick = float(row["live_ticking_avg"])
            by_date.setdefault(d, []).append((ts_et, tick))
    for d in by_date:
        by_date[d].sort()
    return by_date


def intraday_features(intraday_by_date):
    """Build per-day intraday feature dicts. Returns {feature_name: {date: value}}."""
    features = {
        "intraday_eod_tick":    {},
        "intraday_velocity":    {},
        "intraday_range":       {},
        "intraday_live_vs_yday": {},
    }
    for hour in INTRADAY_SAMPLE_HOURS_ET:
        features[f"intraday_tick_at_{hour:02d}_et"] = {}

    for d, ticks in intraday_by_date.items():
        if len(ticks) < 3:
            continue

        prices = [p for _, p in ticks]
        features["intraday_eod_tick"][d] = prices[-1]
        features["intraday_range"][d]    = max(prices) - min(prices)

        first_ts, first_p = ticks[0]
        last_ts,  last_p  = ticks[-1]
        hours = (last_ts - first_ts).total_seconds() / 3600
        if hours > 1:
            features["intraday_velocity"][d] = (last_p - first_p) / hours

        for hour in INTRADAY_SAMPLE_HOURS_ET:
            target  = ticks[0][0].replace(hour=hour, minute=0, second=0, microsecond=0)
            nearest = min(ticks, key=lambda t: abs((t[0] - target).total_seconds()))
            if abs((nearest[0] - target).total_seconds()) <= 90 * 60:
                features[f"intraday_tick_at_{hour:02d}_et"][d] = nearest[1]

    return features


# --- Main analysis --------------------------------------------------------

def main():
    aaa = load_daily_csv(AAA_CSV, AAA_DATE_COL, AAA_PRICE_COL)
    gb  = load_daily_csv(GB_DAILY_CSV, "date", "national_avg_regular")

    aaa_deltas = to_deltas(aaa)
    gb_deltas  = to_deltas(gb)

    common_dates = set(gb) & set(aaa)
    print(f"AAA rows: {len(aaa)}, GasBuddy daily rows: {len(gb)}, overlap: {len(common_dates)}")
    print()

    feature_specs = []

    # Daily delta features at lags 0-7
    for lag in range(0, 8):
        shifted = {}
        for d, v in gb_deltas.items():
            d_new = (dt.date.fromisoformat(d) + dt.timedelta(days=lag)).isoformat()
            shifted[d_new] = v
        feature_specs.append((f"gb_delta_lag_{lag}", shifted))

    # GB-AAA spread (level)
    spread_today = {d: gb[d] - aaa[d] for d in common_dates}
    feature_specs.append(("gb_minus_aaa_spread", spread_today))

    # Intraday features if available
    intraday_by_date = load_intraday_csv(GB_INTRADAY_CSV)
    if len(intraday_by_date) >= MIN_INTRADAY_DAYS:
        print(f"Intraday CSV has {len(intraday_by_date)} days; including intraday features.")
        for fname, fdict in intraday_features(intraday_by_date).items():
            feature_specs.append((fname, fdict))
    else:
        print(f"Intraday CSV has {len(intraday_by_date)} days; need {MIN_INTRADAY_DAYS} "
              f"before intraday features are included.")
    print()

    # Score each feature against next-day AAA delta
    results = []
    for name, feature_dict in feature_specs:
        pairs = align(feature_dict, aaa_deltas, lag_days=1)
        r = pearson(pairs)
        results.append({"feature": name, "pearson_r": r, "n": len(pairs)})

    ranked = sorted(
        [r for r in results if r["pearson_r"] is not None],
        key=lambda x: abs(x["pearson_r"]),
        reverse=True,
    )

    print("=== FEATURES RANKED BY |correlation with next-day AAA delta| ===")
    print(f"{'feature':<40}{'pearson r':<14}{'n'}")
    print("-" * 62)
    for r in ranked:
        print(f"{r['feature']:<40}{r['pearson_r']:+.4f}      {r['n']}")

    # Recent days snapshot
    print()
    print("=== RECENT 7 DAYS ===")
    all_dates = sorted(set(gb) | set(aaa))[-7:]
    for d in all_dates:
        gb_p  = gb.get(d)
        aaa_p = aaa.get(d)
        gb_d  = gb_deltas.get(d)
        aaa_d = aaa_deltas.get(d)
        print(
            f"  {d}  "
            f"GB={gb_p:.3f if gb_p is not None else '—':>6}  "
            f"(Δ{gb_d:+.3f if gb_d is not None else '—':>6})  "
            f"AAA={aaa_p:.3f if aaa_p is not None else '—':>6}  "
            f"(Δ{aaa_d:+.3f if aaa_d is not None else '—':>6})"
        )

    # --- Build enriched JSON for dashboard --------------------------------
    latest_gb_date  = max(gb)  if gb  else None
    latest_aaa_date = max(aaa) if aaa else None
    latest_gb_price  = gb[latest_gb_date]   if latest_gb_date  else None
    latest_aaa_price = aaa[latest_aaa_date] if latest_aaa_date else None

    # GB-AAA spread on the most recent overlapping date
    overlap_dates = sorted(common_dates)
    latest_overlap = overlap_dates[-1] if overlap_dates else None
    gb_aaa_spread = (gb[latest_overlap] - aaa[latest_overlap]) if latest_overlap else None

    # Top signal: most predictive feature + its current value + implied direction
    top_signal = None
    if ranked:
        top = ranked[0]
        # Find the feature dict for the top feature
        top_dict = next((d for n, d in feature_specs if n == top["feature"]), {})
        current_dates = sorted(top_dict)
        current_value = top_dict[current_dates[-1]] if current_dates else None
        implied = None
        if current_value is not None and top["pearson_r"] is not None:
            product = current_value * top["pearson_r"]
            implied = "up" if product > 0.0001 else "down" if product < -0.0001 else "flat"
        top_signal = {
            "feature":       top["feature"],
            "pearson_r":     top["pearson_r"],
            "current_value": current_value,
            "implied_aaa_direction": implied,
        }

    OUTPUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_JSON.write_text(json.dumps({
        "generated_at":                  dt.datetime.utcnow().isoformat(),
        "latest_gb_price":               latest_gb_price,
        "latest_gb_date":                latest_gb_date,
        "latest_aaa_price":              latest_aaa_price,
        "latest_aaa_date":               latest_aaa_date,
        "gb_aaa_spread":                 gb_aaa_spread,
        "overlap_days":                  len(common_dates),
        "min_intraday_days_threshold":   MIN_INTRADAY_DAYS,
        "intraday_days_available":       len(intraday_by_date),
        "top_signal":                    top_signal,
        "ranked_features":               ranked,
    }, indent=2))
    print()
    print(f"Wrote {OUTPUT_JSON}")


if __name__ == "__main__":
    main()
