import os
import json
import logging

import psycopg2
from kafka import KafkaConsumer, TopicPartition

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
logger = logging.getLogger("consumer")

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "flights")

POSTGRES_HOST = os.environ.get("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.environ.get("POSTGRES_PORT", "5432")
POSTGRES_DB = os.environ.get("POSTGRES_DB", "flights")
POSTGRES_USER = os.environ.get("POSTGRES_USER", "flights")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "flights")

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
"""

REQUIRED_FIELDS = ["icao24", "latitude", "longitude"]


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


def build_consumer():
    consumer = KafkaConsumer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        key_deserializer=lambda k: k.decode("utf-8") if k else None,
        enable_auto_commit=False,
    )

    partition_ids = consumer.partitions_for_topic(KAFKA_TOPIC) or {0}
    partitions = [TopicPartition(KAFKA_TOPIC, p) for p in partition_ids]
    consumer.assign(partitions)
    consumer.seek_to_beginning(*partitions)

    logger.info("Manually assigned partitions: %s", partitions)
    return consumer


def build_db_connection():
    return psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
    )


def main():
    consumer = build_consumer()
    conn = build_db_connection()
    conn.autocommit = True

    logger.info("Consumer started, listening on topic '%s'", KAFKA_TOPIC)

    with conn.cursor() as cur:
        for message in consumer:
            record = message.value

            if not is_valid(record):
                logger.warning("Skipping invalid record: %s", record)
                continue

            try:
                cur.execute(INSERT_HISTORY_SQL, record)
                cur.execute(UPSERT_CURRENT_SQL, record)
            except Exception:
                logger.exception("Failed to write record for icao24=%s", record.get("icao24"))
                conn.rollback()


if __name__ == "__main__":
    main()
