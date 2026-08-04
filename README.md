# nornir-dashboard

A lightweight web dashboard for monitoring `nornir-build` runs in real time over
MQTT. It subscribes to the run-scoped topics published by `nornir_shared`,
persists run summaries and event history to SQLite, and serves a single-page web
UI with live updates over a WebSocket.

## What it shows

- A list of runs (live and historical), each identified by a unique `run_id`,
  with pipeline name, volume path, status, and error/warning counts.
- For the selected run: current pipeline, current stage/command, current
  section/element being processed, a percent-complete progress bar, and a
  filterable log pane (errors and warnings highlighted).

Because history is persisted to SQLite, past runs remain viewable after the
build process exits or the broker restarts.

## Architecture

```
nornir-build (publisher)  ->  mosquitto broker  ->  nornir-dashboard
                                                     |- paho subscriber (nornir/run/#)
                                                     |- SQLite store (runs + events)
                                                     |- FastAPI REST + WebSocket
                                                     '- static web UI
browser  <->  dashboard (WebSocket + REST)
```

The browser talks only to the dashboard backend; the dashboard is the single
MQTT subscriber.

## Topic scheme

Publishers send to run-scoped topics:

- `nornir/run/{run_id}/meta` (retained) - run metadata and status
- `nornir/run/{run_id}/log/{info|warning|error|debug}`
- `nornir/run/{run_id}/progress`
- `nornir/run/{run_id}/status`
- `nornir/run/{run_id}/event` - structured stage/iterate events

## Configuration (environment variables)

| Variable | Default | Purpose |
|----------|---------|---------|
| `NORNIR_MQTT_HOST` | `127.0.0.1` | MQTT broker host to subscribe to |
| `NORNIR_MQTT_PORT` | `1883` | MQTT broker port |
| `NORNIR_MQTT_RUN_TOPIC_ROOT` | `nornir/run` | Run topic namespace root |
| `NORNIR_DASHBOARD_DB` | `./nornir-dashboard.db` | SQLite database path |
| `NORNIR_DASHBOARD_HOST` | `0.0.0.0` | HTTP bind host |
| `NORNIR_DASHBOARD_PORT` | `8087` | HTTP port |
| `NORNIR_DASHBOARD_MAX_EVENTS` | `100000` | Max retained events per run in SQLite (`0` disables prune) |
| `NORNIR_DASHBOARD_STALE_AFTER` | `600` | Seconds without traffic before a running build is shown as stale |
| `NORNIR_DASHBOARD_STALE_SWEEP_INTERVAL` | `60` | How often (seconds) to re-check for stale runs |
| `NORNIR_DASHBOARD_RETENTION_DAYS` | `30` | Auto-delete runs with no activity for this many days (`0` disables) |
| `NORNIR_DASHBOARD_RETENTION_SWEEP_INTERVAL` | `86400` | How often (seconds) to run the retention sweeper |

### Log history API and UI

- `GET /api/runs/{run_id}/events` returns a page of events (default newest page).
  Query params: `limit` (max 5000), `after_id`, `before_id` (load older),
  `q` (case-insensitive substring of the stored JSON payload), and `types`
  (comma-separated: `error`, `warning`, `info`, `debug`, `event`, `status`).
  `q` and `types` combine with AND; pagination applies to the filtered set.
  Omit `types` for all kinds (raw API); the UI always sends the checked set.
- `GET /api/runs/{run_id}/events/export` streams the retained transcript as
  plain text (oldest first), honoring the same `q` and `types` filters.
- The UI keeps a bounded DOM window, loads older pages on scroll / **Load older**,
  searches the **full retained** SQLite history (not only visible lines), applies
  level checkboxes on the server for search / load-older / export, and offers
  **Download logs**.

## Running locally

```bash
pip install .
NORNIR_MQTT_HOST=127.0.0.1 nornir-dashboard
# open http://localhost:8087
```

## Running with Docker

See `nornir-docker/compose.dashboard.yaml` for a ready-to-run stack that starts
a mosquitto broker and the dashboard together. From the monorepo root:

```powershell
.\nornir-docker\start-dashboard.ps1
# after changing nornir-builddashboard code:
.\nornir-docker\start-dashboard.ps1 -Rebuild
```
