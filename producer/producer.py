import os
import json
import time
import logging

from kafka import KafkaProducer
from opensky_client import OpenSkyClient

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
logger = logging.getLogger("producer")

LONDON_BBOX = (51.28, -0.51, 51.69, 0.33)

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "flights")
POLL_INTERVAL_SECONDS = int(os.environ.get("POLL_INTERVAL_SECONDS", "30"))

OPENSKY_CLIENT_ID = os.environ.get("OPENSKY_CLIENT_ID")
OPENSKY_CLIENT_SECRET = os.environ.get("OPENSKY_CLIENT_SECRET")

FIELDS = [
    "icao24", "callsign", "origin_country", "time_position", "last_contact",
    "longitude", "latitude", "baro_altitude", "on_ground", "velocity",
    "true_track", "vertical_rate", "sensors", "geo_altitude", "squawk",
    "spi", "position_source",
]


def state_to_dict(state):
    record = dict(zip(FIELDS, state))
    return {
        "icao24": record.get("icao24"),
        "callsign": (record.get("callsign") or "").strip() or None,
        "origin_country": record.get("origin_country"),
        "longitude": record.get("longitude"),
        "latitude": record.get("latitude"),
        "baro_altitude_m": record.get("baro_altitude"),
        "velocity_ms": record.get("velocity"),
        "heading_deg": record.get("true_track"),
        "vertical_rate": record.get("vertical_rate"),
        "on_ground": record.get("on_ground"),
        "time_position": record.get("time_position"),
    }
def build_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        retries=5,
        linger_ms=200,
    )


def main():
    if not OPENSKY_CLIENT_ID or not OPENSKY_CLIENT_SECRET:
        logger.warning("No OPENSKY_CLIENT_ID/SECRET set — running anonymously with reduced rate limits.")

    client = OpenSkyClient(OPENSKY_CLIENT_ID, OPENSKY_CLIENT_SECRET)
    producer = build_producer()

    logger.info("Starting poll loop every %ss for bbox %s", POLL_INTERVAL_SECONDS, LONDON_BBOX)

    while True:
        start = time.time()
        states = client.get_states(bbox=LONDON_BBOX)
        logger.info("Fetched %d aircraft states", len(states))

        published = 0
        for state in states:
            if not state or not state[0]:
                continue
            record = state_to_dict(state)
            if record["latitude"] is None or record["longitude"] is None:
                continue
            producer.send(KAFKA_TOPIC, key=record["icao24"], value=record)
            published += 1

        producer.flush()
        logger.info("Published %d valid records to topic '%s'", published, KAFKA_TOPIC)

        elapsed = time.time() - start
        time.sleep(max(0, POLL_INTERVAL_SECONDS - elapsed))


if __name__ == "__main__":
    main()