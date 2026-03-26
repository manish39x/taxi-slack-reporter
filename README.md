# 🚕 NYC Taxi Daily Slack Reporter

An end-to-end data engineering pipeline that extracts NYC Yellow Taxi trip data, computes daily KPIs, loads them into PostgreSQL, and delivers a formatted report to Slack every morning at 8 AM — fully orchestrated with Kestra.

![Python](https://img.shields.io/badge/Python-3.12-blue?style=flat-square&logo=python)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-336791?style=flat-square&logo=postgresql)
![Kestra](https://img.shields.io/badge/Orchestration-Kestra-blueviolet?style=flat-square)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?style=flat-square&logo=docker)
![Slack](https://img.shields.io/badge/Reports-Slack-4A154B?style=flat-square&logo=slack)

---

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                        Kestra (8 AM cron)                   │
│                                                             │
│   Clone repo → Install deps → run.py                       │
│                                 │                           │
│                    ┌────────────┼────────────┐              │
│                    ▼            ▼            ▼              │
│                Extract      Transform      Load             │
│                    │            │            │              │
│                    └────────────┴────────────┘              │
│                                 │                           │
│                          Slack Webhook                      │
└─────────────────────────────────────────────────────────────┘
```

**Data flow:**

1. `extract.py` — pulls yesterday's raw trips from `yellow_taxi_trips` via SQLAlchemy
2. `transform.py` — computes KPIs: total trips, revenue, avg fare, avg distance, peak hour, top zones, payment split, anomaly detection
3. `load.py` — upserts aggregated results into `daily_summary`, logs run metadata to `pipeline_runs`
4. `notify.py` — builds a Slack Block Kit message with day-over-day trends and POSTs via webhook
5. `run.py` — single entrypoint that chains all four steps with error handling

---

## Tech Stack

| Layer            | Tool                                   |
| ---------------- | -------------------------------------- |
| Language         | Python 3.12                            |
| Data processing  | pandas, SQLAlchemy                     |
| Database         | PostgreSQL 15 (Docker)                 |
| Orchestration    | Kestra (Docker)                        |
| Notifications    | Slack Incoming Webhooks                |
| Source data      | NYC TLC Yellow Taxi Parquet (Jan 2024) |
| Containerization | Docker Compose                         |

---

## Project Structure

```
taxi-slack-reporter/
├── docker-compose.yml        # Postgres + Kestra services
├── .env.example              # Environment variable template
├── setup/
│   ├── init.sql              # Creates all 3 tables on container start
│   └── ingest.py             # Downloads parquet + loads into Postgres
├── pipeline/
│   ├── extract.py            # Pull raw trips for a given date
│   ├── transform.py          # Aggregate into KPIs
│   ├── load.py               # Upsert to daily_summary + log run
│   ├── notify.py             # Build + send Slack Block Kit message
│   └── run.py                # Single entrypoint for orchestration
└── flows/
    └── taxi_daily_report.yml # Kestra flow definition
```

---

## Database Schema

**`yellow_taxi_trips`** — raw ingested data (~2.8M rows for Jan 2024)

| Column             | Type             | Description                   |
| ------------------ | ---------------- | ----------------------------- |
| pickup_datetime    | TIMESTAMP        | Trip start time               |
| dropoff_datetime   | TIMESTAMP        | Trip end time                 |
| passenger_count    | DOUBLE PRECISION | Number of passengers          |
| trip_distance      | DOUBLE PRECISION | Distance in miles             |
| pickup_location_id | INTEGER          | TLC zone ID                   |
| fare_amount        | DOUBLE PRECISION | Base fare (USD)               |
| tip_amount         | DOUBLE PRECISION | Tip (USD)                     |
| total_amount       | DOUBLE PRECISION | Total charged (USD)           |
| payment_type       | BIGINT           | 1=credit, 2=cash, 3=no charge |

**`daily_summary`** — one row per day, written by the pipeline

| Column          | Type             | Description                   |
| --------------- | ---------------- | ----------------------------- |
| report_date     | DATE PK          | The date this row covers      |
| total_trips     | INTEGER          | Count of all trips            |
| total_revenue   | DOUBLE PRECISION | Sum of total_amount           |
| avg_fare        | DOUBLE PRECISION | Avg fare_amount               |
| avg_distance    | DOUBLE PRECISION | Avg trip_distance             |
| peak_hour       | INTEGER          | Hour with most pickups (0–23) |
| peak_hour_trips | INTEGER          | Trip count in peak hour       |
| created_at      | TIMESTAMP        | When this row was written     |

**`pipeline_runs`** — audit log of every pipeline execution

| Column           | Type             | Description                 |
| ---------------- | ---------------- | --------------------------- |
| id               | SERIAL PK        | Run ID                      |
| run_date         | DATE             | Date the pipeline ran for   |
| status           | VARCHAR          | success / failed            |
| rows_processed   | INTEGER          | Raw rows ingested           |
| duration_seconds | DOUBLE PRECISION | How long the run took       |
| error_message    | TEXT             | Null on success             |
| created_at       | TIMESTAMP        | Timestamp of this log entry |

---

## Slack Report Format

Each morning the pipeline posts a Block Kit message to your configured channel:

```
🚕 NYC Taxi Daily Report — 2024-01-15
────────────────────────────────────
Total Trips      Total Revenue
74,789           $1,469,156.13
↑ 4.1% DoD      ↑ 2.8% DoD

Avg Fare         Avg Distance
$19.64           3.80 mi
↓ 1.2% DoD      → flat

🕐 Peak Hour: 2:00 PM with 5,791 trips

Top 5 Pickup Zones
Zone 132   5,437 trips   $64.23 avg
Zone 138   3,412 trips   $40.20 avg
...

Payment Split
💳 Credit card: 60,748 (81%)  💵 Cash: 10,763 (14%)

Pipeline ran at 08:00:04 · Source: PostgreSQL
```

---

## Quickstart

### Prerequisites

- Docker + Docker Compose
- Python 3.12+
- A Slack workspace with Incoming Webhooks enabled

### 1. Clone and configure

```bash
git clone https://github.com/manish39x/taxi-slack-reporter.git
cd taxi-slack-reporter

cp .env.example .env
# Edit .env — fill in your Postgres credentials and Slack webhook URL
```

### 2. Start services

```bash
docker compose up -d
# Postgres + Kestra start; init.sql creates tables automatically
```

### 3. Load the data

```bash
pip install pandas sqlalchemy psycopg2-binary pyarrow python-dotenv

python setup/ingest.py
# Downloads Jan 2024 NYC Taxi parquet (~50MB) and loads ~2.8M rows
# Progress bar shows chunk-by-chunk status + final row count verification
```

### 4. Run the pipeline manually

```bash
pip install requests

python pipeline/run.py 2024-01-15
# Runs extract → transform → load → Slack notification for that date
```

### 5. Schedule with Kestra

Open `http://localhost:8080` → **Flows** → **+** → paste `flows/taxi_daily_report.yml` → **Save**.

The flow clones this repo on each run and executes `pipeline/run.py` with yesterday's date. It runs daily at 8 AM via cron. On failure, it posts an error alert to the same Slack channel.

To trigger a manual test run: **Execute** → set `target_date` to `2024-01-15` → **Run**.

---

## Environment Variables

Copy `.env.example` to `.env` and fill in:

```env
POSTGRES_USER=your_user
POSTGRES_PASSWORD=your_password
POSTGRES_DB=your_db
POSTGRES_HOST=localhost        # use host.docker.internal inside Kestra
SLACK_WEBHOOK_URL=https://hooks.slack.com/services/...
```

> ⚠️ `.env` is gitignored. Never commit it.

In Kestra, set `SLACK_WEBHOOK_URL` as a namespace secret under `taxi.pipeline`.

---

## Key Engineering Decisions

**Chunked ingestion with progress logging** — the ingest script writes in 50,000-row chunks and prints a live progress bar with ETA, rows/sec, and a post-load row count verification.

**Upsert instead of insert** — `daily_summary` uses `ON CONFLICT (report_date) DO UPDATE` so re-running the pipeline for the same date is always safe.

**Numpy type sanitization** — pandas aggregations return `np.float64` which psycopg2 can't serialize. The `_to_python()` function recursively converts the entire metrics dict to native Python types before any DB write.

**Single entrypoint for orchestration** — `run.py` chains all steps and catches exceptions centrally, logging failures to `pipeline_runs` and sending a Slack error alert before exiting with code 1.

**Date-parameterized pipeline** — every step accepts a `target_date` argument, making it trivial to backfill any historical date without touching the code.

---

## Backfilling Multiple Days

```bash
for date in 2024-01-10 2024-01-11 2024-01-12 2024-01-13 2024-01-14; do
    python pipeline/run.py $date
done
```

---

## What's Next

- [ ] Add dbt models for the transform layer
- [ ] Zone ID → zone name lookup using the TLC zone lookup CSV
- [ ] Weekly summary report (Mon 8 AM) alongside the daily one
- [ ] Grafana dashboard connected to `daily_summary`
- [ ] Extend to Green Taxi and FHV data

---

## Data Source

NYC TLC Yellow Taxi Trip Records — January 2024  
`https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet`  
Published by the NYC Taxi & Limousine Commission under public license.
