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