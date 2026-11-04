import os
import json
import time
import logging

import psycopg2
from psycopg2.extras import execute_batch
from kafka import KafkaConsumer, TopicPartition

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
logger = logging.getLogger("consumer")

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "flights")

# A full connection string (e.g. from Neon/Supabase) takes precedence over the
# individual POSTGRES_* settings, so the same image can write to the local
# Docker Postgres or a hosted one.
DATABASE_URL = os.environ.get("DATABASE_URL") or None
POSTGRES_HOST = os.environ.get("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.environ.get("POSTGRES_PORT", "5432")
POSTGRES_DB = os.environ.get("POSTGRES_DB", "flights")
POSTGRES_USER = os.environ.get("POSTGRES_USER", "flights")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "flights")

# How long to keep rows in the append-only history table. Hosted free tiers
# have small storage caps, so old history is pruned periodically. 0 = keep forever.
HISTORY_RETENTION_HOURS = float(os.environ.get("HISTORY_RETENTION_HOURS", "24"))
PRUNE_INTERVAL_SECONDS = 600

# Schema file applied on startup (every statement is IF NOT EXISTS), so a fresh
# hosted database needs no manual setup. Mounted from sql/init.sql by Compose.
SCHEMA_PATH = os.environ.get("SCHEMA_PATH", os.path.join(os.path.dirname(__file__), "init.sql"))

INSERT_HISTORY_SQL = """
    INSERT INTO flight_positions (
        icao24, callsign, origin_country, longitude, latitude,
        baro_altitude_m, velocity_ms, heading_deg, vertical_rate,
        on_ground, time_position
    ) VALUES (
        %(icao24)s, %(callsign)s, %(origin_country)s, %(longitude)s, %(latitude)s,
        %(baro_altitude_m)s, %(velocity_ms)s, %(heading_deg)s, %(vertical_rate)s,
        %(on_ground)s, %(time_position)s
    )
"""

# The WHERE clause stops an older observation from overwriting a newer one
# (e.g. if messages are ever replayed or arrive out of order).
UPSERT_CURRENT_SQL = """
    INSERT INTO current_flights (
        icao24, callsign, origin_country, longitude, latitude,
        baro_altitude_m, velocity_ms, heading_deg, vertical_rate,
        on_ground, time_position, updated_at
    ) VALUES (
        %(icao24)s, %(callsign)s, %(origin_country)s, %(longitude)s, %(latitude)s,
        %(baro_altitude_m)s, %(velocity_ms)s, %(heading_deg)s, %(vertical_rate)s,
        %(on_ground)s, %(time_position)s, now()
    )
    ON CONFLICT (icao24) DO UPDATE SET
        callsign = EXCLUDED.callsign,
        origin_country = EXCLUDED.origin_country,
        longitude = EXCLUDED.longitude,
        latitude = EXCLUDED.latitude,
        baro_altitude_m = EXCLUDED.baro_altitude_m,
        velocity_ms = EXCLUDED.velocity_ms,
        heading_deg = EXCLUDED.heading_deg,
        vertical_rate = EXCLUDED.vertical_rate,
        on_ground = EXCLUDED.on_ground,
        time_position = EXCLUDED.time_position,
        updated_at = now()
    WHERE current_flights.time_position IS NULL
       OR EXCLUDED.time_position IS NULL
       OR EXCLUDED.time_position >= current_flights.time_position
"""

LOAD_OFFSETS_SQL = "SELECT partition, next_offset FROM kafka_offsets WHERE topic = %s"

SAVE_OFFSET_SQL = """
    INSERT INTO kafka_offsets (topic, partition, next_offset)
    VALUES (%s, %s, %s)
    ON CONFLICT (topic, partition) DO UPDATE SET next_offset = EXCLUDED.next_offset
"""

PRUNE_HISTORY_SQL = "DELETE FROM flight_positions WHERE ingested_at < now() - make_interval(secs => %s)"

REQUIRED_FIELDS = ["icao24", "latitude", "longitude"]
COLUMNS = [
    "icao24", "callsign", "origin_country", "longitude", "latitude",
    "baro_altitude_m", "velocity_ms", "heading_deg", "vertical_rate",
    "on_ground", "time_position",
]


def normalize(record):
    """Keep only known columns and fill missing optional ones with None, so a
    message missing a field can't break the batch insert."""
    return {col: record.get(col) for col in COLUMNS}


def is_valid(record):
    if not isinstance(record, dict):
        return False
    for field in REQUIRED_FIELDS:
        if record.get(field) is None:
            return False
    if not (-90 <= record["latitude"] <= 90):
        return False
    if not (-180 <= record["longitude"] <= 180):
        return False
    return True


def build_db_connection():
    if DATABASE_URL:
        return psycopg2.connect(DATABASE_URL)
    return psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
    )


def ensure_schema(conn):
    if not os.path.exists(SCHEMA_PATH):
        logger.warning("Schema file %s not found; assuming tables already exist", SCHEMA_PATH)
        return
    with open(SCHEMA_PATH) as f, conn.cursor() as cur:
        cur.execute(f.read())
    conn.commit()
    logger.info("Schema ensured from %s", SCHEMA_PATH)


def load_offsets(conn):
    with conn.cursor() as cur:
        cur.execute(LOAD_OFFSETS_SQL, (KAFKA_TOPIC,))
        rows = cur.fetchall()
    conn.commit()
    return dict(rows)


def build_consumer(saved_offsets):
    """Manually assign all partitions and resume from the offsets stored in
    Postgres. Offsets live in the database (not in a Kafka consumer group) and
    are committed in the same transaction as the data, so a restart neither
    replays nor skips messages."""
    consumer = KafkaConsumer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        key_deserializer=lambda k: k.decode("utf-8") if k else None,
        enable_auto_commit=False,
    )

    partition_ids = consumer.partitions_for_topic(KAFKA_TOPIC) or {0}
    partitions = [TopicPartition(KAFKA_TOPIC, p) for p in partition_ids]
    consumer.assign(partitions)

    for tp in partitions:
        if tp.partition in saved_offsets:
            consumer.seek(tp, saved_offsets[tp.partition])
            logger.info("Resuming %s at offset %s", tp, saved_offsets[tp.partition])
        else:
            consumer.seek_to_beginning(tp)
            logger.info("No stored offset for %s, starting from beginning", tp)

    return consumer


def write_batch(conn, records, next_offsets):
    """Write one polled batch and its offsets atomically."""
    with conn.cursor() as cur:
        if records:
            execute_batch(cur, INSERT_HISTORY_SQL, records)
            execute_batch(cur, UPSERT_CURRENT_SQL, records)
        for partition, offset in next_offsets.items():
            cur.execute(SAVE_OFFSET_SQL, (KAFKA_TOPIC, partition, offset))
    conn.commit()


def prune_history(conn):
    if HISTORY_RETENTION_HOURS <= 0:
        return
    with conn.cursor() as cur:
        cur.execute(PRUNE_HISTORY_SQL, (HISTORY_RETENTION_HOURS * 3600,))
        deleted = cur.rowcount
    conn.commit()
    if deleted:
        logger.info("Pruned %d history rows older than %sh", deleted, HISTORY_RETENTION_HOURS)


def main():
    conn = build_db_connection()
    ensure_schema(conn)
    consumer = build_consumer(load_offsets(conn))
    logger.info("Consumer started, listening on topic '%s'", KAFKA_TOPIC)

    last_prune = 0.0
    while True:
        batches = consumer.poll(timeout_ms=1000, max_records=500)

        records, next_offsets = [], {}
        for tp, messages in batches.items():
            for message in messages:
                if is_valid(message.value):
                    records.append(normalize(message.value))
                else:
                    logger.warning("Skipping invalid record: %s", message.value)
            next_offsets[tp.partition] = messages[-1].offset + 1

        if next_offsets:
            # On failure the process exits and Docker restarts it; since the
            # offsets weren't committed, the batch is simply retried.
            write_batch(conn, records, next_offsets)
            logger.info("Wrote %d records", len(records))

        if time.time() - last_prune > PRUNE_INTERVAL_SECONDS:
            prune_history(conn)
            last_prune = time.time()


if __name__ == "__main__":
    main()
