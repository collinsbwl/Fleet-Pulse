-- Runs once, when the Postgres volume is first created (docker-entrypoint-initdb.d).
-- To re-run: make down && make up.

-- One row per vehicle: its most recent known state. The map's initial load reads this.
CREATE TABLE vehicles_latest (
    vehicle_id    text             PRIMARY KEY,
    event_ts      timestamptz      NOT NULL,  -- when the vehicle produced the reading
    lat           double precision NOT NULL,
    lon           double precision NOT NULL,
    speed_kph     real             NOT NULL,
    fuel_pct      real             NOT NULL,
    engine_temp_c real             NOT NULL,
    updated_at    timestamptz      NOT NULL DEFAULT now()  -- when the server wrote it
);

-- Every reading ever received (append-only). Used for drill and capacity checks.
-- The primary key makes redelivered messages (at-least-once) safe to skip with
-- ON CONFLICT DO NOTHING, and doubles as the (vehicle_id, event_ts) lookup index.
CREATE TABLE positions_history (
    vehicle_id    text             NOT NULL,
    event_ts      timestamptz      NOT NULL,
    lat           double precision NOT NULL,
    lon           double precision NOT NULL,
    speed_kph     real             NOT NULL,
    fuel_pct      real             NOT NULL,
    engine_temp_c real             NOT NULL,
    ingested_at   timestamptz      NOT NULL DEFAULT now(),
    PRIMARY KEY (vehicle_id, event_ts)
);

-- Time-window queries across all vehicles ("rows written during the drill").
CREATE INDEX positions_history_ingested_at_idx ON positions_history USING brin (ingested_at);
