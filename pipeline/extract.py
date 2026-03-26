import os
import time
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

DB_URL = (
  f"postgresql://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}"
  f"@localhost:5432/{os.getenv('POSTGRES_DB')}"
)

def log(msg):
  ts = time.strftime("%H:%M:%S")
  print(f"[{ts}] {msg}")

def get_engine():
  return create_engine(DB_URL)

def extract(target_date:str = None) -> pd.DataFrame:
  """
  Pull all trips for targeted date from postgreSQL.
  target_date format: 'YYYY-MM-DD'
  DEFAULTS to yesterday if not provided. 
  """
  if target_date is None:
    target_date = (pd.Timestamp.today() - pd.Timedelta(days=1)).strftime("%Y-%m-%d")
  
  log(f"Extracting trips for date: {target_date}")
  start = time.time()

  query = text("""
    SELECT
      pickup_datetime,
      dropoff_datetime,
      passenger_count,
      trip_distance,
      pickup_location_id,
      fare_amount,
      tip_amount,
      total_amount,
      payment_type
    FROM yellow_taxi_trips
    WHERE DATE(pickup_datetime) = :target_date
  """)

  engine = get_engine()
  with engine.connect() as conn:
    df = pd.read_sql(query, conn, params={"target_date":target_date})
  
  elapsed = time.time() - start

  if df.empty:
    log(f"WARNING: No rows found for {target_date}")
    return df
  
  log(f"EXTRACTED {len(df):,} rows in {elapsed:.2f}s")
  _log_sample(df, target_date)

  return df

def _log_sample(df:pd.DataFrame, target_date:str):
  """Print a quick sanity check after extraction."""
  print()
  print("─" * 50)
  print(f"  Date              : {target_date}")
  print(f"  Rows extracted    : {len(df):,}")
  print(f"  Columns           : {list(df.columns)}")
  print(f"  Pickup time range : {df['pickup_datetime'].min()} → {df['pickup_datetime'].max()}")
  print(f"  Fare range        : ${df['fare_amount'].min():.2f} → ${df['fare_amount'].max():.2f}")
  print(f"  Null check        :")
  for col in ["pickup_datetime", "fare_amount", "trip_distance", "total_amount"]:
      nulls = df[col].isna().sum()
      status = "✓" if nulls == 0 else f"✗ {nulls:,} nulls"
      print(f"    {col:<20} {status}")
  print("─" * 50)
  print()


if __name__ == "__main__":
    import sys

    # Pass a date as argument for testing: python extract.py 2024-01-15
    date_arg = sys.argv[1] if len(sys.argv) > 1 else None
    df = extract(date_arg)

    if not df.empty:
        log("Extract complete. Ready for transform step.")