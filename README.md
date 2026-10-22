# Fleet Pulse

A real-time fleet telemetry pipeline built to be operated, not just demoed. Simulated vehicles stream GPS positions through Redpanda into PostgreSQL and onto a live Mapbox map. SLOs, burn-rate alerts, end-to-end tracing, failure drills and a capacity report show how it holds up and where it breaks.

## Architecture

```
simulator (Go) ──► Redpanda ──► server (Go) ──► PostgreSQL
                   topic:          │
                   positions       ├─ WebSocket ──► web/map.html (Mapbox GL JS)
                                   ├─ GET /vehicles
                                   │
                                   ├─ /metrics ──────────► Prometheus ─┐
                                   └─ OTLP traces + logs ► OTel Collector ─► Tempo, Loki ─┴─► Grafana
```

| Component | Role |
| --- | --- |
| `simulator` | One goroutine per vehicle doing a random walk in a city bounding box; publishes Protobuf to `positions`, keyed by vehicle ID |
| Redpanda | Kafka-compatible broker; `positions` has 6 partitions and 1 h retention |
| `server` | Consumes `positions`, batch-writes to Postgres, pushes updates over WebSocket, exposes `/metrics`, `/healthz`, `/readyz` |
| PostgreSQL | `vehicles_latest` (current state per vehicle) and `positions_history` (every reading, deduplicated on `vehicle_id, event_ts`) |
| `web/map.html` | Mapbox GL JS with clustering; vehicles silent for more than 60 s turn grey |

**Stack:** Go · Redpanda (franz-go) · PostgreSQL (pgx) · Protobuf · Mapbox GL JS · Prometheus · Grafana · OpenTelemetry · Tempo · Loki · Docker Compose

## Quick start

**Prerequisites:** Docker Desktop or OrbStack (about 4 GB RAM free), Go 1.24+, and a free [Mapbox access token](https://account.mapbox.com/access-tokens/).

```bash
make up        # start the stack and create topics
make psql      # open a SQL shell on the fleet database
make down      # stop everything and delete its data
```

Run `make help` to list every target.

| Service | URL | Credentials |
| --- | --- | --- |
| Redpanda Console | http://localhost:8081 | none |
| PostgreSQL | `localhost:5432`, database `fleet` | `fleet` / `fleet` (local only) |
| Kafka API (host clients) | `localhost:19092` | none |
| Live map | http://localhost:8080 | none |
| Grafana | http://localhost:3000 | none |
| Prometheus | http://localhost:9090 | none |

## SLOs

| SLO | SLI | Target | Window |
| --- | --- | --- | --- |
| Freshness | Updates pushed ≤ 2 s after the vehicle's event timestamp ÷ all updates | 99% | 7 days |
| API availability | Successful `GET /vehicles` and WebSocket upgrades ÷ attempts | 99.5% | 7 days |

Alerts use multi-window, multi-burn-rate rules: page at 14.4× (1 h and 5 m), ticket at 6× (6 h and 30 m). Every alert links to a runbook.

## Observability

- **Metrics:** RED per endpoint, a freshness histogram, consumer lag per partition, messages/s, Postgres write latency and Go runtime stats.
- **Traces:** one trace follows a message from simulator → Redpanda → server → Postgres → WebSocket, with context carried in Kafka record headers.
- **Logs:** structured JSON with `trace_id` on every line.
- **Correlation:** freshness exemplars link to traces in Tempo, and traces link to their logs in Loki.

Grafana dashboards (SLO overview, pipeline, service health) are provisioned as code.

## Failure drills

Each drill has a runbook and a postmortem recording time to detect and time to recover. Recovery is measured from removing the fault until consumer lag is back to baseline.

| Drill | Fault |
| --- | --- |
| Consumer kill | `docker kill` the server mid-stream |
| Postgres outage | Pause the database for 2 minutes |
| Redpanda outage | Stop the broker for 2 minutes |
| Topic flood | Run the simulator at 10× its normal rate |

## Capacity

A load ramp steps from 1,000 to 50,000 vehicles and records throughput, lag, freshness and resource use at each step. The capacity report states messages/s per server instance, the resource that saturates first, and how the system degrades past that point.

## Repo layout

```
cmd/          simulator and server binaries
internal/     shared packages (kafka, store, ws, telemetry, ...)
proto/        Protobuf schema
web/          map page
deploy/       Docker Compose and config for every container
docs/         runbooks, postmortems, capacity report
scripts/      drill and load scripts
Makefile
```

## License

MIT
