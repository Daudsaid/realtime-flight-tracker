CREATE TABLE IF NOT EXISTS flight_positions (
    id              BIGSERIAL PRIMARY KEY,
    icao24          VARCHAR(10)      NOT NULL,
    callsign        VARCHAR(20),
    origin_country  VARCHAR(100),
    longitude       DOUBLE PRECISION,
    latitude        DOUBLE PRECISION,
    baro_altitude_m DOUBLE PRECISION,
    velocity_ms     DOUBLE PRECISION,
    heading_deg     DOUBLE PRECISION,
    vertical_rate   DOUBLE PRECISION,
    on_ground       BOOLEAN,
    time_position   BIGINT,
    ingested_at     TIMESTAMPTZ      NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS current_flights (
    icao24          VARCHAR(10) PRIMARY KEY,
    callsign        VARCHAR(20),
    origin_country  VARCHAR(100),
    longitude       DOUBLE PRECISION,
    latitude        DOUBLE PRECISION,
    baro_altitude_m DOUBLE PRECISION,
    velocity_ms     DOUBLE PRECISION,
    heading_deg     DOUBLE PRECISION,
    vertical_rate   DOUBLE PRECISION,
    on_ground       BOOLEAN,
    time_position   BIGINT,
    updated_at      TIMESTAMPTZ      NOT NULL DEFAULT now()
);

-- Next Kafka offset to read per partition. Written in the same transaction as
-- the flight rows, so the consumer resumes exactly where it left off.
CREATE TABLE IF NOT EXISTS kafka_offsets (
    topic       TEXT    NOT NULL,
    partition   INTEGER NOT NULL,
    next_offset BIGINT  NOT NULL,
    PRIMARY KEY (topic, partition)
);

-- Supports pruning old history and the dashboard's "seen in the last N minutes" filter.
CREATE INDEX IF NOT EXISTS idx_flight_positions_ingested_at ON flight_positions (ingested_at);
CREATE INDEX IF NOT EXISTS idx_current_flights_updated_at ON current_flights (updated_at);
