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


def load(metrics: dict) -> bool:
    """
    Writes aggregated metrics to daily_summary table.
    Returns True on success, False on failure.
    """
    if not metrics:
        log("ERROR: Empty metrics dict — nothing to load.")
        return False

    report_date = metrics["report_date"]
    log(f"Loading summary for {report_date} into daily_summary...")
    start = time.time()

    engine = get_engine()

    # ── Upsert: replace if date already exists ────────────────
    upsert_sql = text("""
        INSERT INTO daily_summary (
            report_date,
            total_trips,
            total_revenue,
            avg_fare,
            avg_distance,
            peak_hour,
            peak_hour_trips,
            created_at
        ) VALUES (
            :report_date,
            :total_trips,
            :total_revenue,
            :avg_fare,
            :avg_distance,
            :peak_hour,
            :peak_hour_trips,
            NOW()
        )
        ON CONFLICT (report_date)
        DO UPDATE SET
            total_trips     = EXCLUDED.total_trips,
            total_revenue   = EXCLUDED.total_revenue,
            avg_fare        = EXCLUDED.avg_fare,
            avg_distance    = EXCLUDED.avg_distance,
            peak_hour       = EXCLUDED.peak_hour,
            peak_hour_trips = EXCLUDED.peak_hour_trips,
            created_at      = NOW()
    """)

    with engine.begin() as conn:
        conn.execute(upsert_sql, {
            "report_date":     report_date,
            "total_trips":     metrics["total_trips"],
            "total_revenue":   metrics["total_revenue"],
            "avg_fare":        metrics["avg_fare"],
            "avg_distance":    metrics["avg_distance"],
            "peak_hour":       metrics["peak_hour"],
            "peak_hour_trips": metrics["peak_hour_trips"],
        })

    elapsed = time.time() - start
    log(f"daily_summary written in {elapsed:.2f}s")

    # ── Log this pipeline run ─────────────────────────────────
    log_run(
        report_date=report_date,
        status="success",
        rows_processed=metrics["total_trips"],
        duration_seconds=elapsed,
        error_message=None,
    )

    _verify(engine, report_date)
    return True


def log_run(
    report_date: str,
    status: str,
    rows_processed: int = 0,
    duration_seconds: float = 0.0,
    error_message: str = None,
):
    """Write a row to pipeline_runs — called on both success and failure."""
    engine = get_engine()
    sql = text("""
        INSERT INTO pipeline_runs (
            run_date, status, rows_processed, duration_seconds, error_message, created_at
        ) VALUES (
            :run_date, :status, :rows_processed, :duration_seconds, :error_message, NOW()
        )
    """)
    with engine.begin() as conn:
        conn.execute(sql, {
            "run_date":         report_date,
            "status":           status,
            "rows_processed":   rows_processed,
            "duration_seconds": round(duration_seconds, 1),
            "error_message":    error_message,
        })
    log(f"Pipeline run logged — status: {status}")


def _verify(engine, report_date: str):
    """Read back the row we just wrote and print it."""
    sql = text("""
        SELECT
            report_date,
            total_trips,
            total_revenue,
            avg_fare,
            avg_distance,
            peak_hour,
            peak_hour_trips,
            created_at
        FROM daily_summary
        WHERE report_date = :report_date
    """)
    with engine.connect() as conn:
        row = conn.execute(sql, {"report_date": report_date}).fetchone()

    if row is None:
        log("ERROR: Row not found after insert — something went wrong.")
        return

    print()
    print("─" * 52)
    print("  Verified row in daily_summary:")
    print(f"    report_date     : {row.report_date}")
    print(f"    total_trips     : {row.total_trips:,}")
    print(f"    total_revenue   : ${row.total_revenue:,.2f}")
    print(f"    avg_fare        : ${row.avg_fare:.2f}")
    print(f"    avg_distance    : {row.avg_distance:.2f} mi")
    print(f"    peak_hour       : {row.peak_hour:02d}:00")
    print(f"    peak_hour_trips : {row.peak_hour_trips:,}")
    print(f"    created_at      : {row.created_at}")
    print("─" * 52)
    print()


if __name__ == "__main__":
    import sys
    from extract import extract
    from transform import transform

    date_arg = sys.argv[1] if len(sys.argv) > 1 else "2024-01-15"

    df      = extract(date_arg)
    metrics = transform(df, date_arg)
    success = load(metrics)

    if success:
        log("Load complete. Pipeline run logged. Ready for Step 5.")