import os
import time
import requests
from dotenv import load_dotenv

load_dotenv()

WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")

def log(msg):
    ts = time.strftime("%H:%M:%S")
    print(f"[{ts}] {msg}")


def _trend(current, previous, prefix=""):
    """Return trend arrow and % change string."""
    if previous is None or previous == 0:
        return "─ no prev data"
    change = ((current - previous) / previous) * 100
    arrow  = "↑" if change >= 0 else "↓"
    sign   = "+" if change >= 0 else ""
    return f"{arrow} {sign}{change:.1f}% DoD"


def _peak_label(hour: int) -> str:
    """Convert 24h hour to readable label e.g. 14 → 2:00 PM"""
    suffix = "AM" if hour < 12 else "PM"
    h      = hour if hour <= 12 else hour - 12
    h      = 12 if h == 0 else h
    return f"{h}:00 {suffix}"


def build_message(metrics: dict, prev: dict = None) -> dict:
    """
    Build a Slack Block Kit message from metrics dict.
    prev is the previous day's metrics for trend calculation (optional).
    """
    date         = metrics["report_date"]
    total_trips  = metrics["total_trips"]
    revenue      = metrics["total_revenue"]
    avg_fare     = metrics["avg_fare"]
    avg_distance = metrics["avg_distance"]
    peak_hour    = metrics["peak_hour"]
    peak_trips   = metrics["peak_hour_trips"]
    top_zones    = metrics.get("top_zones", [])
    payment      = metrics.get("payment_split", {})
    anomaly      = metrics.get("anomaly")

    # Trends (only if previous day data exists)
    p = prev or {}
    trip_trend    = _trend(total_trips,  p.get("total_trips"),    )
    revenue_trend = _trend(revenue,      p.get("total_revenue"),  )
    fare_trend    = _trend(avg_fare,     p.get("avg_fare"),       )
    dist_trend    = _trend(avg_distance, p.get("avg_distance"),   )

    # Top zones as a markdown table
    zone_lines = ["*Zone ID*   *Trips*   *Avg Fare*"]
    for z in top_zones:
        zone_lines.append(
            f"`{z['pickup_location_id']:<6}`   "
            f"{z['trips']:>6,}   "
            f"${z['avg_fare']:.2f}"
        )
    zones_text = "\n".join(zone_lines)

    # Payment split
    cc      = payment.get("credit_card", 0)
    cash    = payment.get("cash", 0)
    other   = sum(v for k, v in payment.items() if k not in ("credit_card", "cash"))
    total_p = cc + cash + other or 1
    payment_text = (
        f"💳 Credit card: *{cc:,}* ({cc/total_p*100:.0f}%)   "
        f"💵 Cash: *{cash:,}* ({cash/total_p*100:.0f}%)   "
        f"Other: *{other:,}*"
    )

    blocks = [
        # ── Header ───────────────────────────────────────────
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": f"🚕 NYC Taxi Daily Report — {date}",
                "emoji": True
            }
        },
        {"type": "divider"},

        # ── KPI fields ───────────────────────────────────────
        {
            "type": "section",
            "fields": [
                {
                    "type": "mrkdwn",
                    "text": f"*Total Trips*\n{total_trips:,}\n_{trip_trend}_"
                },
                {
                    "type": "mrkdwn",
                    "text": f"*Total Revenue*\n${revenue:,.2f}\n_{revenue_trend}_"
                },
                {
                    "type": "mrkdwn",
                    "text": f"*Avg Fare*\n${avg_fare:.2f}\n_{fare_trend}_"
                },
                {
                    "type": "mrkdwn",
                    "text": f"*Avg Distance*\n{avg_distance:.2f} mi\n_{dist_trend}_"
                },
            ]
        },
        {"type": "divider"},

        # ── Peak hour ────────────────────────────────────────
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": (
                    f"🕐 *Peak Hour:* {_peak_label(peak_hour)} "
                    f"with *{peak_trips:,}* trips"
                )
            }
        },

        # ── Top zones ────────────────────────────────────────
        {"type": "divider"},
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*Top 5 Pickup Zones*\n{zones_text}"
            }
        },

        # ── Payment split ────────────────────────────────────
        {"type": "divider"},
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*Payment Split*\n{payment_text}"
            }
        },
    ]

    # ── Anomaly alert (only if flagged) ──────────────────────
    if anomaly:
        blocks += [
            {"type": "divider"},
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"⚠️ *Anomaly Detected*\n{anomaly}"
                }
            }
        ]

    # ── Footer ───────────────────────────────────────────────
    blocks.append({
        "type": "context",
        "elements": [
            {
                "type": "mrkdwn",
                "text": (
                    f"Pipeline ran at {time.strftime('%H:%M:%S')} · "
                    f"Source: PostgreSQL · "
                    f"Next run: tomorrow 8:00 AM"
                )
            }
        ]
    })

    return {"blocks": blocks}


def send(metrics: dict, prev: dict = None) -> bool:
    """Build and POST the Slack message. Returns True on success."""
    if not WEBHOOK_URL:
        log("ERROR: SLACK_WEBHOOK_URL not set in .env")
        return False

    log("Building Slack message...")
    payload = build_message(metrics, prev)

    log("Sending to Slack...")
    resp = requests.post(WEBHOOK_URL, json=payload, timeout=10)

    if resp.status_code == 200 and resp.text == "ok":
        log("Message delivered successfully.")
        return True
    else:
        log(f"ERROR: Slack returned {resp.status_code} — {resp.text}")
        return False


def send_error_alert(date: str, error: str) -> bool:
    """Send a failure alert to Slack when the pipeline crashes."""
    if not WEBHOOK_URL:
        return False

    payload = {
        "blocks": [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": f"🚨 Pipeline Failed — {date}",
                    "emoji": True
                }
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*Error:*\n```{error}```"
                }
            },
            {
                "type": "context",
                "elements": [
                    {
                        "type": "mrkdwn",
                        "text": f"Failed at {time.strftime('%H:%M:%S')} · Check pipeline logs"
                    }
                ]
            }
        ]
    }

    resp = requests.post(WEBHOOK_URL, json=payload, timeout=10)
    return resp.status_code == 200


if __name__ == "__main__":
    import sys
    from extract import extract
    from transform import transform
    from load import load

    date_arg = sys.argv[1] if len(sys.argv) > 1 else "2024-01-15"

    # Run full pipeline
    df      = extract(date_arg)
    metrics = transform(df, date_arg)
    load(metrics)

    # Try to get previous day for trends
    prev_date = (
        __import__("pandas").Timestamp(date_arg) - __import__("pandas").Timedelta(days=1)
    ).strftime("%Y-%m-%d")

    try:
        prev_df      = extract(prev_date)
        prev_metrics = transform(prev_df, prev_date)
    except Exception:
        prev_metrics = None

    send(metrics, prev_metrics)