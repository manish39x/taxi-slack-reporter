import sys
import time
import traceback
from extract import extract
from transform import transform
from load import load, log_run
from notify import send, send_error_alert

def main():
    date_arg = sys.argv[1] if len(sys.argv) > 1 else None
    if date_arg is None:
        import pandas as pd
        date_arg = (pd.Timestamp.today() - pd.Timedelta(days=1)).strftime("%Y-%m-%d")

    total_start = time.time()
    print(f"\n{'='*52}")
    print(f"  Pipeline starting for date: {date_arg}")
    print(f"{'='*52}\n")

    try:
        # ── Extract ───────────────────────────────────────────
        df = extract(date_arg)
        if df.empty:
            raise ValueError(f"No data found for {date_arg}")

        # ── Transform ─────────────────────────────────────────
        metrics = transform(df, date_arg)
        if not metrics:
            raise ValueError("Transform returned empty metrics")

        # ── Load ──────────────────────────────────────────────
        success = load(metrics)
        if not success:
            raise RuntimeError("Load step failed")

        # ── Previous day for trends ───────────────────────────
        import pandas as pd
        prev_date = (
            pd.Timestamp(date_arg) - pd.Timedelta(days=1)
        ).strftime("%Y-%m-%d")
        try:
            prev_df      = extract(prev_date)
            prev_metrics = transform(prev_df, prev_date)
        except Exception:
            prev_metrics = None

        # ── Notify ────────────────────────────────────────────
        send(metrics, prev_metrics)

        total_secs = time.time() - total_start
        print(f"\n{'='*52}")
        print(f"  Pipeline complete in {total_secs:.1f}s")
        print(f"{'='*52}\n")

    except Exception as e:
        error_msg = traceback.format_exc()
        print(f"\nPIPELINE ERROR:\n{error_msg}")
        log_run(
            report_date=date_arg,
            status="failed",
            error_message=str(e),
        )
        send_error_alert(date_arg, str(e))
        sys.exit(1)

if __name__ == "__main__":
    main()