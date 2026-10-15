# GPU Job Scheduler

A distributed GPU job scheduling system written in **Go**, with three switchable
scheduling algorithms, atomic preemption, and a full Prometheus/Grafana
observability stack — all brought up with a single `docker-compose up`.

![Go](https://img.shields.io/badge/Go-1.22-00ADD8?style=flat&logo=go) ![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-336791?style=flat&logo=postgresql) ![Redis](https://img.shields.io/badge/Redis-7-DC382D?style=flat&logo=redis) ![Prometheus](https://img.shields.io/badge/Prometheus-2.54-E6522C?style=flat&logo=prometheus) ![Grafana](https://img.shields.io/badge/Grafana-11.2-F46800?style=flat&logo=grafana)

---

## Screenshots

**Live dashboard — 5 running jobs, all 3 nodes saturated, PRIORITY algorithm active**
![Grafana dashboard](assets/grafana-dashboard.png)

**Mid-run state — queue draining, a job completing, p95 wait climbing under load**
![Grafana dashboard mid-run](assets/grafana-preemption-state.png)

**Metrics pipeline — Prometheus scraping `/metrics` and computing p95 job wait time**
![Prometheus p95 wait query](assets/prometheus-p95-wait.png)

**Preemption demo — filling the cluster with LOW jobs, switching to PRIORITY, then submitting a HIGH job**
![Terminal running the preemption repro script](assets/terminal-preemption-demo.png)

---

## What it does

A REST API accepts GPU job submissions. A background scheduler goroutine
(2-second tick) continuously assigns queued jobs to virtual GPU nodes, completes
jobs whose estimated duration has elapsed, and publishes live metrics.

**Three scheduling algorithms, switchable at runtime via the API — no restart
required:**

| Algorithm | Behaviour |
|---|---|
| `FIFO` | Jobs run in submission order |
| `SJF` | Shortest estimated-duration job runs first, regardless of submission order |
| `PRIORITY` | HIGH → MEDIUM → LOW. A queued HIGH job that can't fit will **preempt** a running MEDIUM/LOW job: the victim is atomically requeued and the freed slots go to the HIGH job in the same tick |

**Cluster topology:** 3 virtual GPU nodes, 10 total slots (`node-01`: 4,
`node-02`: 4, `node-03`: 2). Each job declares how many slots it needs (1–4)
and an estimated duration (5–300s).

---

## Architecture

```
┌──────────────────────────────────────────────────────────────┐
│                       Docker Compose                         │
│                                                                │
│  ┌──────────┐    ┌────────────────────────────────────────┐  │
│  │  Client  │───▶│            Go API (chi) :8080           │  │
│  └──────────┘    │  ┌────────────┐      ┌──────────────┐   │  │
│                  │  │  Handlers  │      │   /metrics   │   │  │
│                  │  └─────┬──────┘      └──────┬───────┘   │  │
│                  │        │                    │           │  │
│                  │  ┌─────▼───────────┐        │           │  │
│                  │  │   Scheduler     │        │           │  │
│                  │  │   goroutine     │        │           │  │
│                  │  │  (2s tick)      │        │           │  │
│                  │  │  FIFO/SJF/PRI   │        │           │  │
│                  │  │  + preemption   │        │           │  │
│                  │  └──┬──────────┬───┘        │           │  │
│                  └─────┼──────────┼─────────────┼──────────┘  │
│                        │          │             │              │
│              ┌─────────▼──┐   ┌───▼────┐        │              │
│              │ PostgreSQL │   │ Redis  │        │              │
│              │  :5432     │   │ :6379  │        │              │
│              └────────────┘   └────────┘        │              │
│                                                  │              │
│                  ┌─────────────┐   ┌─────────────▼──────┐      │
│                  │   Grafana   │◀──│    Prometheus       │      │
│                  │   :3000     │   │     :9090            │      │
│                  └─────────────┘   └──────────────────────┘      │
└──────────────────────────────────────────────────────────────┘
```

**PostgreSQL** — authoritative job and node state. Every state transition
(assign, complete, preempt) runs inside a single `pgx` transaction so a job's
status and its node's `available_slots` count move together — the slot count
can never drift out of sync with reality, even under concurrent ticks.

**Redis** — persists the active scheduling algorithm (`scheduler:algorithm`)
and the running preemption counter (`scheduler:preemptions`). On startup the
scheduler reads the last algorithm from Redis before its first tick, so a
`PRIORITY` switch survives a process/container restart.

**Prometheus** — scrapes `/metrics` every 5s. All job-count gauges
(`gpu_jobs_running`, `gpu_jobs_queued_by_priority`, `gpu_node_utilization_ratio`)
are recomputed from PostgreSQL on every 2-second scheduler tick rather than
trusted purely from in-process increments, so they stay correct even
immediately after a restart (verified — see [Verification](#verification)).

| Metric | Type | Why it matters |
|---|---|---|
| `gpu_jobs_running` | Gauge | Live execution count, reconciled from the DB each tick |
| `gpu_jobs_queued_by_priority` | GaugeVec | Queue backpressure per tier |
| `gpu_job_wait_seconds` | Histogram | p95 wait time — catches starvation |
| `gpu_jobs_total` | CounterVec | Completed-job throughput by priority |
| `gpu_preemptions_total` | Counter | Preemption rate under `PRIORITY` mode |
| `gpu_node_utilization_ratio` | GaugeVec | Per-node slot fill (0.0–1.0) |

(A seventh, `gpu_scheduler_algorithm_active`, flags which algorithm is live and
drives the Grafana "Active Algorithm" panel.)

---

## Quickstart

**Prerequisites:** Docker Desktop (or Docker Engine) + Docker Compose.

```bash
git clone https://github.com/<your-username>/gpu-scheduler
cd gpu-scheduler
docker-compose up --build
```

All five services start, the PostgreSQL schema migrates automatically from
`migrations/init.sql`, and the Grafana dashboard provisions itself.

| Service | URL | Credentials |
|---|---|---|
| Grafana Dashboard | http://localhost:3000 | admin / admin |
| REST API | http://localhost:8080 | — |
| Prometheus | http://localhost:9090 | — |

> The `postgres` / `admin` credentials in `docker-compose.yml` are local
> development defaults only — this stack is not configured for a production
> deployment.

> If the build fails with `go: updates to go.mod needed; to update it: go mod
> tidy`, run `docker run --rm -v "$PWD":/app -w /app golang:1.22-alpine go mod
> tidy` once to sync `go.mod`/`go.sum`, then re-run `docker-compose up --build`.

For a full step-by-step walkthrough (including running natively without
Docker), see [Local Setup & Testing](#local-setup--testing-macos--vs-code)
below.

---

## API Reference

### Submit a job
```
POST /jobs
```
```json
{
  "name": "bert-training-run-42",
  "priority": "HIGH",
  "gpu_slots_required": 2,
  "estimated_duration_seconds": 45
}
```
`priority`: `HIGH` | `MEDIUM` | `LOW` (default: `MEDIUM`)
`gpu_slots_required`: 1–4
`estimated_duration_seconds`: 5–300

### List / inspect / cancel jobs
```
GET    /jobs?status=RUNNING&priority=HIGH&limit=20
GET    /jobs/{id}
DELETE /jobs/{id}        # only QUEUED jobs can be cancelled
```

### Switch scheduling algorithm (live, no restart needed)
```
PUT /scheduler/algorithm
```
```json
{ "algorithm": "PRIORITY" }
```

### Cluster & scheduler state
```
GET /stats       # job counts by status/priority, node state, preemption total
GET /nodes       # per-node slot capacity and usage
GET /scheduler   # active algorithm + preemption count
GET /health
```

---

## Reproducing preemption

```bash
# 1. Fill all 10 cluster slots with LOW priority jobs
for i in $(seq 1 5); do
  curl -s -X POST http://localhost:8080/jobs \
    -H "Content-Type: application/json" \
    -d "{\"name\":\"low-job-$i\",\"priority\":\"LOW\",\"gpu_slots_required\":2,\"estimated_duration_seconds\":120}" | jq .status
done

# 2. Switch to PRIORITY
curl -X PUT http://localhost:8080/scheduler/algorithm \
  -H "Content-Type: application/json" -d '{"algorithm":"PRIORITY"}'

# 3. Submit a HIGH priority job — scheduler preempts a LOW job for it
curl -X POST http://localhost:8080/jobs \
  -H "Content-Type: application/json" \
  -d '{"name":"critical-inference","priority":"HIGH","gpu_slots_required":2,"estimated_duration_seconds":20}'

# 4. The preempted LOW job is back in QUEUED, preempted_count incremented
curl -s http://localhost:8080/jobs?status=QUEUED | jq '.[] | {name, priority, preempted_count}'

# 5. Preemption count (Redis-backed, survives restarts)
curl -s http://localhost:8080/scheduler | jq .preemptions
```

Watch `gpu_preemptions_total` increment on the Grafana dashboard in real time.

---

## Job lifecycle

```
QUEUED ──▶ RUNNING ──▶ COMPLETED
              │
              └──▶ (preempted: requeued atomically, preempted_count++) ──▶ QUEUED
QUEUED ──▶ FAILED   (manual cancel via DELETE /jobs/{id})
```

Scheduler tick (every 2s):
1. Complete any `RUNNING` jobs whose `estimated_duration` has elapsed.
2. Pull `QUEUED` jobs and sort them by the active algorithm.
3. Assign jobs to nodes with available slots — each assignment is one atomic
   DB transaction (status update + slot deduction).
4. In `PRIORITY` mode, if a HIGH job doesn't fit, find a running MEDIUM/LOW
   job on a node that would free enough slots, preempt it (one atomic
   transaction: requeue the victim + restore its slots), then immediately
   assign the HIGH job.
5. Recompute queue-depth, node-utilization, and running-job gauges directly
   from PostgreSQL, so Prometheus always reflects real DB state.

---

## Verification

This is what was actually checked against a running instance, not assumed
from the code:

| Claim | Verified by |
|---|---|
| 3 nodes / 10 total slots | Filled the cluster with 5× 2-slot jobs; confirmed all 3 nodes at 0 available slots |
| FIFO, SJF, PRIORITY all switchable live | Switched algorithms via `PUT /scheduler/algorithm` mid-run, no restart |
| SJF picks shortest job first, ignoring submission order | Queued a 250s job, then an 8s job; freed one slot; the 8s job ran, the 250s job stayed queued |
| HIGH preempts a running LOW job via one DB transaction | Filled the cluster with LOW jobs, submitted a HIGH job, confirmed the LOW victim returned to `QUEUED` with `preempted_count=1` in a single tick |
| Preemptions tracked in Redis + exposed over REST | Read `scheduler:preemptions` directly from Redis and from `GET /scheduler`; values matched |
| Algorithm persists across a restart | Set `PRIORITY`, killed the process, restarted it against the same Postgres/Redis — it came back up already in `PRIORITY` |
| 6 core Prometheus metrics present and correctly typed | Scraped `/metrics` directly, confirmed Counter/Gauge/Histogram types and label sets |
| Node slot accounting never drifts | Ran preemption + concurrent assignment cycles; `SUM(available_slots)` plus slots held by `RUNNING` jobs always equalled 10 |

**One real bug was found and fixed during this verification:** the
`gpu_jobs_running` gauge was updated only via in-process `Inc()`/`Dec()` calls,
so after a process restart it reset to 0 even while jobs were still `RUNNING`
in PostgreSQL (confirmed: killed the process with 5 jobs running, restarted
it, the metric read `0` against a real DB count of `5`). It's now recomputed
from PostgreSQL on every scheduler tick, the same pattern already used for the
queue-depth and node-utilization gauges — re-verified after the fix and the
gauge now matches the DB immediately after restart.

---

## Grafana Dashboard Panels

| Panel | Prometheus Query |
|---|---|
| Running Jobs | `gpu_jobs_running` |
| Total In Queue | `sum(gpu_jobs_queued_by_priority)` |
| Completed Jobs | `sum(gpu_jobs_total{status="COMPLETED"})` |
| Preemptions | `gpu_preemptions_total` |
| Queue Depth by Priority | `gpu_jobs_queued_by_priority{priority=~"HIGH\|MEDIUM\|LOW"}` |
| Job Wait Time p95 | `histogram_quantile(0.95, rate(gpu_job_wait_seconds_bucket[2m]))` |
| Node Utilization | `gpu_node_utilization_ratio{node=~"node-.*"}` |
| Active Algorithm | `gpu_scheduler_algorithm_active` |

---

## Project layout

```
.
├── main.go                      # wiring: DB pool, Redis client, scheduler, HTTP server
├── internal/
│   ├── models/job.go             # Job, Node, request/response types
│   ├── db/postgres.go            # all SQL — job CRUD, atomic assign/complete/preempt
│   ├── cache/redis.go            # algorithm + preemption-counter persistence
│   ├── scheduler/scheduler.go    # the scheduling loop, FIFO/SJF/PRIORITY, preemption
│   ├── metrics/metrics.go        # Prometheus metric definitions
│   └── handlers/handlers.go      # REST API (chi router)
├── migrations/init.sql           # schema + seed node data
├── docker-compose.yml            # app + postgres + redis + prometheus + grafana
├── Dockerfile                    # multi-stage Go build
├── prometheus.yml                # scrape config
└── grafana/                      # auto-provisioned datasource + dashboard
```

---

## Local Setup & Testing (macOS + VS Code)

### 1. Install prerequisites

```bash
# Docker Desktop (includes Docker Compose v2)
brew install --cask docker
open -a Docker        # wait for the whale icon in the menu bar to stop animating

# Go (only needed if you want to run the binary natively, outside Docker)
brew install go

# Optional but used in the example commands below
brew install jq
```

Open the project folder in VS Code (`code .`) and install the **Go**
extension (`golang.go`) if you'll be editing the Go code.

### 2. Run the full stack

```bash
cd gpu-scheduler
docker-compose up --build
```

Wait for the logs to show:
```
app_1  | API listening on :8080
app_1  | [scheduler] started — tick every 2s
```

### 3. Verify it's actually running

```bash
curl -s http://localhost:8080/health | jq
# {"algorithm":"FIFO","status":"ok"}

curl -s http://localhost:8080/nodes | jq
# 3 nodes, 10 total slots
```

### 4. Exercise the scheduler

```bash
# Submit a job
curl -s -X POST http://localhost:8080/jobs \
  -H "Content-Type: application/json" \
  -d '{"name":"test-job","priority":"MEDIUM","gpu_slots_required":2,"estimated_duration_seconds":15}' | jq

# Watch it get assigned (scheduler ticks every 2s)
sleep 3
curl -s http://localhost:8080/jobs | jq '.[0] | {name, status, node_name}'

# Run through the full preemption scenario from "Reproducing preemption" above
```

### 5. View the dashboard

Open http://localhost:3000 (admin/admin) → the "GPU Scheduler" dashboard is
already provisioned. Submit a few jobs and switch algorithms via `curl` to
watch the panels update live.

### 6. Run it natively instead of in Docker (optional)

Useful for debugging in VS Code with breakpoints.

```bash
# Start only the dependencies
docker-compose up -d postgres redis

# Run the Go binary directly, pointed at them
export DATABASE_URL="postgres://postgres:postgres@localhost:5432/gpuscheduler?sslmode=disable"
export REDIS_URL="redis://localhost:6379"
go run .
```

Set the same two environment variables in VS Code's `launch.json` to debug
with breakpoints:
```json
{
  "version": "0.2.0",
  "configurations": [
    {
      "name": "Launch gpu-scheduler",
      "type": "go",
      "request": "launch",
      "mode": "auto",
      "program": "${workspaceFolder}",
      "env": {
        "DATABASE_URL": "postgres://postgres:postgres@localhost:5432/gpuscheduler?sslmode=disable",
        "REDIS_URL": "redis://localhost:6379"
      }
    }
  ]
}
```

### 7. Tear down

```bash
docker-compose down          # stop containers, keep data volumes
docker-compose down -v       # stop and wipe Postgres/Grafana data too
```

### Troubleshooting

| Symptom | Fix |
|---|---|
| `port is already allocated` on 5432/6379/8080/9090/3000 | Something else on your Mac is already using that port — stop it, or change the left-hand side of the port mapping in `docker-compose.yml` |
| `app` container restarts in a loop | Run `docker-compose logs app` — almost always means Postgres/Redis weren't healthy yet; `docker-compose up` already waits on their healthchecks, so check `docker-compose logs postgres redis` |
| Grafana shows "No data" | Prometheus scrapes `app:8080` every 5s — give it ~10s after startup, and confirm `http://localhost:9090/targets` shows the `gpu-scheduler` target as `UP` |
| `go run .` can't resolve `localhost` for Postgres/Redis | Make sure you ran `docker-compose up -d postgres redis` first and are using `localhost`, not the `postgres`/`redis` service names (those only resolve inside the Compose network) |

---

## Skills demonstrated

- **Go** — goroutines, mutexes, context propagation, `net/http`, chi router
- **PostgreSQL** — schema design, ACID transactions, indexed queries, ENUM types
- **Redis** — persisted scheduler state, atomic counters
- **Prometheus** — Counter, Gauge, Histogram metric types; p95 histogram quantile; DB-reconciled gauges
- **Grafana** — auto-provisioned dashboard via JSON
- **Docker** — multi-stage Go build, healthcheck-gated service startup, Compose orchestration
- **Scheduling algorithms** — FIFO, SJF, Priority with preemption
- **Systems design** — atomic slot accounting, scheduler-as-goroutine, clean separation of DB/cache/scheduler/HTTP layers
