CREATE TABLE IF NOT EXISTS yellow_taxi_trips (
    id                SERIAL PRIMARY KEY,
    pickup_datetime   TIMESTAMP,
    dropoff_datetime  TIMESTAMP,
    passenger_count   INTEGER,
    trip_distance     NUMERIC(6,2),
    pickup_location_id INTEGER,
    fare_amount       NUMERIC(8,2),
    tip_amount        NUMERIC(8,2),
    total_amount      NUMERIC(8,2),
    payment_type      INTEGER
);

CREATE INDEX idx_pickup_date ON yellow_taxi_trips (DATE(pickup_datetime));

CREATE TABLE IF NOT EXISTS daily_summary (
    report_date      DATE PRIMARY KEY,
    total_trips      INTEGER,
    total_revenue    NUMERIC(12,2),
    avg_fare         NUMERIC(8,2),
    avg_distance     NUMERIC(6,2),
    peak_hour        INTEGER,
    peak_hour_trips  INTEGER,
    created_at       TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS pipeline_runs (
    id               SERIAL PRIMARY KEY,
    run_date         DATE,
    status           VARCHAR(20),
    rows_processed   INTEGER,
    duration_seconds NUMERIC(6,1),
    error_message    TEXT,
    created_at       TIMESTAMP DEFAULT NOW()
);