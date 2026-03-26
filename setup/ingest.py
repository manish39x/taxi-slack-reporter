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

PARQUET_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet"

COLUMN_MAP = {
    "tpep_pickup_datetime":  "pickup_datetime",
    "tpep_dropoff_datetime": "dropoff_datetime",
    "passenger_count":       "passenger_count",
    "trip_distance":         "trip_distance",
    "PULocationID":          "pickup_location_id",
    "fare_amount":           "fare_amount",
    "tip_amount":            "tip_amount",
    "total_amount":          "total_amount",
    "payment_type":          "payment_type",
}

CHUNK_SIZE = 50_000

def log(msg):
    ts = time.strftime("%H:%M:%S")
    print(f"[{ts}] {msg}")

def ingest():
    total_start = time.time()

    # ── Download ──────────────────────────────────────────────
    log("Downloading parquet file...")
    dl_start = time.time()
    df = pd.read_parquet(PARQUET_URL, columns=list(COLUMN_MAP.keys()))
    log(f"Download complete — {len(df):,} rows in {time.time()-dl_start:.1f}s")

    # ── Rename ────────────────────────────────────────────────
    df = df.rename(columns=COLUMN_MAP)

    # ── Cleanup ───────────────────────────────────────────────
    log("Cleaning data...")
    before = len(df)
    df = df[df["fare_amount"] > 0]
    df = df[df["trip_distance"] > 0]
    df = df[df["total_amount"] > 0]
    df = df.dropna(subset=["pickup_datetime", "dropoff_datetime"])
    dropped = before - len(df)
    log(f"Cleanup done — kept {len(df):,} rows, dropped {dropped:,} bad rows")

    # ── Chunked insert with progress ─────────────────────────
    engine = create_engine(DB_URL)
    total_rows = len(df)
    total_chunks = (total_rows // CHUNK_SIZE) + 1
    rows_written = 0
    insert_start = time.time()

    log(f"Starting insert — {total_chunks} chunks of {CHUNK_SIZE:,} rows each")
    print()  # blank line before chunk logs

    for i, start in enumerate(range(0, total_rows, CHUNK_SIZE), start=1):
        chunk = df.iloc[start : start + CHUNK_SIZE]
        chunk_start = time.time()

        chunk.to_sql(
            "yellow_taxi_trips",
            engine,
            if_exists="append",
            index=False,
            chunksize=CHUNK_SIZE,
            method="multi",
        )

        rows_written += len(chunk)
        chunk_secs = time.time() - chunk_start
        elapsed = time.time() - insert_start
        pct = (rows_written / total_rows) * 100
        rate = rows_written / elapsed if elapsed > 0 else 0
        eta = (total_rows - rows_written) / rate if rate > 0 else 0

        bar_filled = int(pct / 5)
        bar = "█" * bar_filled + "░" * (20 - bar_filled)

        print(
            f"  Chunk {i:>3}/{total_chunks}  [{bar}] {pct:>5.1f}%  "
            f"{rows_written:>9,}/{total_rows:,}  "
            f"{chunk_secs:.1f}s/chunk  ETA {eta:.0f}s",
            end="\r" if i < total_chunks else "\n"
        )

    # ── Verify count in DB ────────────────────────────────────
    print()
    log("Verifying row count in database...")
    with engine.connect() as conn:
        result = conn.execute(text("SELECT COUNT(*) FROM yellow_taxi_trips"))
        db_count = result.scalar()

    total_secs = time.time() - total_start

    # ── Final summary ─────────────────────────────────────────
    print()
    print("─" * 55)
    print(f"  Rows in DataFrame  : {total_rows:,}")
    print(f"  Rows in PostgreSQL : {db_count:,}")
    print(f"  Match              : {'✓ yes' if db_count == total_rows else '✗ MISMATCH'}")
    print(f"  Total time         : {total_secs:.1f}s")
    print(f"  Avg insert rate    : {total_rows / total_secs:,.0f} rows/sec")
    print("─" * 55)

if __name__ == "__main__":
    ingest()
