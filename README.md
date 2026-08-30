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
| `NORNIR_DASHBOARD_HOST` | `127.0.0.1` | HTTP bind host (the container image sets `0.0.0.0`) |
| `NORNIR_DASHBOARD_PORT` | `8087` | HTTP port |
| `NORNIR_DASHBOARD_MAX_EVENTS` | `0` | Max retained events per run in SQLite (`0` = unlimited / no prune) |
| `NORNIR_DASHBOARD_STALE_AFTER` | `600` | Seconds without traffic before a running build is shown as stale (always on; `<=0` falls back to `600`) |
| `NORNIR_DASHBOARD_STALE_SWEEP_INTERVAL` | `60` | How often (seconds) to re-check for stale runs (`<=0` falls back to `60`) |
| `NORNIR_DASHBOARD_RETENTION_DAYS` | `30` | Auto-delete runs with no activity for this many days (`0` disables) |
| `NORNIR_DASHBOARD_RETENTION_SWEEP_INTERVAL` | `86400` | How often (seconds) to run the retention sweeper (`<=0` falls back to `86400`) |
| `NORNIR_DASHBOARD_TOKEN` | *(unset)* | When set, every `/api/*` request and the WebSocket must present this token |
| `NORNIR_DASHBOARD_ALLOW_DELETE` | `1` | Set to `0` to refuse `DELETE /api/runs/{run_id}` |

Malformed numeric values log a warning and fall back to the default rather than
aborting startup.

### Access control

The dashboard binds to loopback by default and has no user accounts. Exposing it
on a network interface means exposing run history — and, unless deletion is
disabled, the ability to destroy it — so when `NORNIR_DASHBOARD_HOST` is not
loopback, set `NORNIR_DASHBOARD_TOKEN` (the app logs a warning if you do not).

Clients may present the token as `Authorization: Bearer <token>`, as an
`X-Dashboard-Token` header, or as a `?token=` query parameter. Open the UI once
as `http://host:8087/?token=<token>`; the page stores it in `sessionStorage` and
attaches it to subsequent API and WebSocket calls. `/`, `/static/*`, and
`/api/health` are not gated, so the page can load and container health probes
work without a token.

`GET /api/health` returns `{"ok": true, "clients": N}` and is what the image's
`HEALTHCHECK` polls.

### Log history API and UI

- `GET /api/runs/{run_id}/events` returns a page of events (default newest page).
  Query params: `limit` (max 5000), `after_id`, `before_id` (load older),
  `q` (case-insensitive substring of the stored JSON payload), and `types`
  (comma-separated: `error`, `warning`, `info`, `debug`, `event`, `status`,
  `other`; `other` covers events whose topic leaf did not map to a known kind).
  A `types` value containing no recognized key falls back to all kinds.
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

## Tests

```bash
pip install -e '.[test]'   # fastapi/uvicorn plus httpx for fastapi.testclient
python -m pytest tests
```

The app-level tests skip themselves (rather than failing collection) when
`fastapi` or `httpx` is missing.

## Logging

The dashboard logs to stdout via `logging.basicConfig` so Docker captures it. If
`NORNIR_LOG_ROOT` is set **and** `nornir_shared` is importable — that is, when
running from a monorepo working copy rather than the standalone image — it hands
logging to `nornir_shared.misc.SetupLogging` so the run joins the unified Nornir
log session instead of this package inventing its own file paths.

## Running with Docker

See `nornir-docker/compose.dashboard.yaml` for a ready-to-run stack that starts
a mosquitto broker and the dashboard together. From the monorepo root:

```powershell
.\nornir-docker\start-dashboard.ps1
# after changing nornir-builddashboard code:
.\nornir-docker\start-dashboard.ps1 -Rebuild
```
