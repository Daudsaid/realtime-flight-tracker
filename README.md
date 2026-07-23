# ✈️ Live Flight Tracker — London

A real-time data engineering pipeline that tracks live aircraft over London, built end-to-end with a streaming architecture: **OpenSky Network API → Apache Kafka (KRaft mode) → PostgreSQL → Streamlit**. Runs entirely locally via Docker Compose.

![Live Flight Tracker Dashboard](docs/screenshot-1.png)

## Overview

This project polls the [OpenSky Network](https://opensky-network.org/) API every 30 seconds for live aircraft state vectors over a London bounding box, streams them through a Kafka topic, persists them into PostgreSQL with both a full history log and a continuously upserted "current state" table, and visualizes the result on an interactive map dashboard.

It was built as a hands-on, file-by-file learning project to understand each layer of a streaming data pipeline — not generated as a finished template — with a particular focus on the operational realities of running Kafka and its clients locally (KRaft configuration, listener networking, dependency compatibility, and consumer group behavior).

## Architecture

```
OpenSky Network API --> Producer (Python) --> Kafka (KRaft) --> Consumer (Python) --> PostgreSQL --> Streamlit Dashboard
```

![Architecture diagram](docs/flight_pipeline_architecture.png)
```

**Data flow:**
1. **Producer** authenticates with OpenSky via OAuth2 (client credentials flow), polls the `/states/all` endpoint every 30 seconds for a bounding box covering Greater London, and publishes each aircraft's state as a JSON message to the Kafka topic `flights`, keyed by `icao24` (the aircraft's unique transponder ID).
2. **Kafka** (single-node, KRaft mode — no ZooKeeper) buffers the stream and decouples the producer from the consumer.
3. **Consumer** reads from the `flights` topic and writes each valid record to two Postgres tables: an append-only history log and an upserted "latest known state per aircraft" table.
4. **Streamlit dashboard** queries the current-state table and renders live aircraft positions on an interactive map, with metrics and a sortable data table, auto-refreshing every 30 seconds.

## Tech Stack

| Layer | Technology | Why |
|---|---|---|
| Message broker | Apache Kafka 3.7.0 (KRaft mode) | Industry-standard streaming platform; KRaft removes the ZooKeeper dependency |
| Producer/Consumer | Python 3.12, `kafka-python-ng` | Pure-Python Kafka client; `-ng` fork used for Python 3.12 compatibility |
| Database | PostgreSQL 16 | Reliable relational store with native upsert support (`ON CONFLICT`) |
| Dashboard | Streamlit + PyDeck | Rapid interactive dashboarding with deck.gl-powered map rendering |
| Orchestration | Docker Compose | Single-command local multi-service orchestration |
| DB browser | Adminer | Lightweight web-based Postgres inspection |

## Project Structure

```
flight-pipeline/
├── docker-compose.yml       # Orchestrates all six services
├── .env                     # OpenSky API credentials (not committed)
├── sql/
│   └── init.sql             # Postgres schema (runs on first container start)
├── producer/
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── opensky_client.py    # OAuth2 auth, token caching, retry/backoff
│   └── producer.py          # Poll loop, transform, publish to Kafka
├── consumer/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── consumer.py          # Kafka consume loop, validation, dual writes
├── dashboard/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── app.py                # Streamlit map + metrics + data table
├── tests/
│   ├── test_producer.py      # Unit tests for state_to_dict()
│   └── test_consumer.py      # Unit tests for is_valid()
└── docs/                     # Screenshots referenced in this README
```

## Database Schema

Two tables, serving different purposes:

- **`flight_positions`** — append-only log. Every message consumed from Kafka is inserted as a new row, preserving the full history of every position observation.
- **`current_flights`** — upserted, one row per aircraft (`icao24` as primary key). Always reflects each aircraft's most recently known state. This is what the dashboard queries, avoiding the need to aggregate the full history table on every page load.

## Getting Started

### Prerequisites

- Docker Desktop (macOS/Windows/Linux)
- An OpenSky Network account with API client credentials ([opensky-network.org](https://opensky-network.org/) → Account → API Client)

### Setup

1. Clone the repository:
   ```bash
   git clone <your-repo-url>
   cd flight-pipeline
   ```

2. Create a `.env` file in the project root with your OpenSky API credentials:
   ```
   OPENSKY_CLIENT_ID=your_client_id_here
   OPENSKY_CLIENT_SECRET=your_client_secret_here
   ```

3. Build and start all services:
   ```bash
   docker compose up --build -d
   ```

4. Open the dashboard at **http://localhost:8501**

### Other endpoints

| Service | URL | Notes |
|---|---|---|
| Streamlit dashboard | http://localhost:8501 | Main UI |
| Adminer (DB browser) | http://localhost:8081 | Server: `postgres`, user/pass/db: `flights` |
| Kafka broker | `localhost:9092` (internal), `localhost:29092` (host) | For external tools like a local Kafka CLI |
| PostgreSQL | `localhost:5432` | Connect via any Postgres client (e.g. DBeaver) using the `flights`/`flights`/`flights` credentials |

### Useful commands

```bash
# View logs for a specific service
docker compose logs -f producer

# Check status of all services
docker compose ps

# Stop everything
docker compose down

# Stop and wipe the Postgres volume (fresh start)
docker compose down -v
```

## Testing

Unit tests cover the two pure-logic functions in the pipeline — the producer's OpenSky state-vector transformation and the consumer's record validation. Kafka, Postgres, and HTTP I/O are intentionally not mocked or unit-tested here, since that logic is a thin wrapper around external services already exercised end-to-end by the running pipeline itself.

```bash
python3.12 -m venv .venv
source .venv/bin/activate
pip install -r producer/requirements.txt -r consumer/requirements.txt -r requirements-dev.txt
PYTHONPATH=. python -m pytest tests/ -v
```

**14 tests**, covering:
- `producer/producer.py` — `state_to_dict()`: field mapping, callsign whitespace handling, blank/missing callsign, null coordinate preservation
- `consumer/consumer.py` — `is_valid()`: required-field checks, non-dict input rejection, latitude/longitude range validation including exact boundary values (±90°/±180°)

## Verification

**All six services running in Docker Desktop:**

![Docker containers running](docs/Screenshot-4-docker.png)

**Sample analytical query — top 10 fastest aircraft currently tracked:**

![Fastest aircraft query](docs/Screenshot-6-fastest-aircraft.png)

**Ingestion throughput — position updates per minute, sustained over an hour+:**

![Minute-bucket ingestion volume](docs/Screenshot-5-minute-bucket.png)

## Key Engineering Decisions & Challenges

A few non-obvious problems encountered while building this, documented for anyone hitting the same issues:

- **Kafka dual-listener configuration.** A single `ADVERTISED_LISTENERS` pointing at `localhost` breaks inter-container connectivity — containers on the Docker network need to resolve the broker via its service name (`kafka`), not `localhost`. Solved with two named listeners: `PLAINTEXT://kafka:9092` for internal Docker traffic and `HOST://localhost:29092` for host-machine tools.
- **`kafka-python` incompatibility with Python 3.12.** The original `kafka-python` package (last released 2020) fails to import under Python 3.12 due to changes in `importlib`. Fixed by switching to the actively maintained `kafka-python-ng` fork, a drop-in replacement under the same `kafka` import namespace.
- **Startup race condition.** Kafka's KRaft broker takes a couple of seconds to become ready after the container starts; `depends_on` in Compose only guarantees container start order, not service readiness. Solved with a `restart: on-failure` policy on the producer and consumer, so they automatically retry until Kafka is actually accepting connections.
- **Silent consumer group coordination hang.** Using a `group_id`-based `KafkaConsumer` caused the consumer to hang indefinitely during the JoinGroup/SyncGroup rebalance handshake, with no error output, due to a broker-version misdetection in the client library. Resolved by switching to manual partition assignment (`consumer.assign()` + `seek_to_beginning()`), which is appropriate here since there's only ever a single consumer instance and no need for dynamic rebalancing.

## Possible Extensions

- Aircraft trail/flight-path visualization instead of point-in-time dots
- Historical playback using the `flight_positions` history table
- Altitude/speed distribution charts
- Alerting on specific aircraft or unusual patterns (e.g. squawk codes)
- Horizontal scaling with multiple consumer instances and proper consumer-group partitioning

## Author

**Daud Abdi**

📧 daudsaidabdi@gmail.com
📱 07389 766509
🔗 [github.com/Daudsaid](https://github.com/Daudsaid)
🔗 [linkedin.com/in/daudabdi0506](https://linkedin.com/in/daudabdi0506)
🌐 [daudabdi.com](https://daudabdi.com)

## License

MIT
