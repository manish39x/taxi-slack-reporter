import time
import pandas as pd

def log(msg):
  ts = time.strftime("%H:%M:%S")
  print(f"[{ts}] {msg}")

def transform(df: pd.DataFrame, target_date: str) -> dict:
  """
    Takes raw trips DataFrame for a single day.
    Returns a dict of aggregated metrics ready for load + reporting.
  """
  if df.empty:
    log("ERROR: Empty DataFrame passed to transform.")
    return {}
  
  log(f"Transforming {len(df):,} rows for {target_date}...")
  start = time.time()

  #─────── CORE KPIs ─────────────────────────────────────────────
  total_trips   = int(len(df))
  total_revenue = round(df["fare_amount"].sum(), 2)
  avg_fare      = round(df["fare_amount"].mean(), 2)
  avg_distance  = round(df["trip_distance"].mean(), 2)

  #─────── Peak hour ─────────────────────────────────────────────
  df["pickup_hour"] = pd.to_datetime(df["pickup_datetime"]).dt.hour
  hourly = df.groupby("pickup_hour").size()
  peak_hour = int(hourly.idxmax())
  peak_hour_trips = int(hourly.max())

  #─────── Top 5 pickup zones ─────────────────────────────────────────────
  top_zones = (
    df.groupby("pickup_location_id")
    .agg(
      trips=("pickup_location_id", "count"),
      avg_fare=("fare_amount", "mean"),
    )
    .sort_values("trips", ascending=False)
    .head(5)
    .reset_index()
  )
  top_zones["avg_fare"] = top_zones["avg_fare"].round(2)

  #─────── Payment split ─────────────────────────────────────────────
  payment_map = {1: "credit_card", 2: "cash", 3: "no_charge", 4: "dispute"}
  payment_counts = (
      df["payment_type"]
      .map(payment_map)
      .fillna("unknown")
      .value_counts()
      .to_dict()
  )

  #─────── Anomaly flag ─────────────────────────────────────────────
  # Flag if avg fare is unusually low or high (basic threshold check)
  anomaly = None
  if avg_fare < 5.0:
    anomaly = f"Avg fare ${avg_fare} is unusually low (threshold: $5.00)"
  elif avg_fare > 20.0:
    anomaly = f"Avg fare ${avg_fare} is unusually high (threshold: $5.00)"
  
  elapsed = time.time() - start
  log(f"Transform complete in {elapsed:.2f}s")

  metrics = {
    "report_date": target_date,
    "total_trips": total_trips,
    "total_revenue":   total_revenue,
    "avg_fare":        avg_fare,
    "avg_distance":    avg_distance,
    "peak_hour":       peak_hour,
    "peak_hour_trips": peak_hour_trips,
    "top_zones":       top_zones.to_dict(orient="records"),
    "payment_split":   payment_counts,
    "anomaly":         anomaly,
  }
  
  metrics = _to_python(metrics)
  _log_summary(metrics)
  return metrics

def _log_summary(m: dict):
  top = m["top_zones"]
  print()
  print("─" * 52)
  print(f"  Date           : {m['report_date']}")
  print(f"  Total trips    : {m['total_trips']:,}")
  print(f"  Total revenue  : ${m['total_revenue']:,.2f}")
  print(f"  Avg fare       : ${m['avg_fare']:.2f}")
  print(f"  Avg distance   : {m['avg_distance']:.2f} mi")
  print(f"  Peak hour      : {m['peak_hour']:02d}:00  ({m['peak_hour_trips']:,} trips)")
  print()
  print("  Top pickup zones:")
  print(f"    {'Zone ID':<10} {'Trips':>8}  {'Avg fare':>9}")
  print(f"    {'─'*7:<10} {'─'*5:>8}  {'─'*8:>9}")
  for z in top:
      print(f"    {z['pickup_location_id']:<10} {z['trips']:>8,}  ${z['avg_fare']:>8.2f}")
  print()
  print("  Payment split:")
  for k, v in m["payment_split"].items():
      print(f"    {k:<15} {v:>8,}")
  print()
  anomaly_line = m["anomaly"] if m["anomaly"] else "✓ none"
  print(f"  Anomaly        : {anomaly_line}")
  print("─" * 52)
  print()


def _to_python(obj):
  """Recursively convert numpy types to native Python types."""
  import numpy as np
  if isinstance(obj, dict):
    return {k: _to_python(v) for k, v in obj.items()}
  elif isinstance(obj, list):
    return [_to_python(i) for i in obj]
  elif isinstance(obj, np.integer):
    return int(obj)
  elif isinstance(obj, np.floating):
    return float(obj)
  else:
    return obj

if __name__ == "__main__":
  import sys
  from extract import extract

  date_arg = sys.argv[1] if len(sys.argv) > 1 else "2024-01-15"
  df = extract(date_arg)
  metrics = transform(df, date_arg)