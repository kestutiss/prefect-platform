# Prefect Platform

Local Prefect 3.x data pipeline platform running on Docker Compose.

## Prerequisites

- Docker Desktop
- `make` (Git Bash / WSL on Windows) — or run the `docker compose` commands directly

## Quick start

```bash
cp .env.example .env
make up          # starts postgres + prefect-server + prefect-worker
```

Open the Prefect UI at **http://localhost:4200**

The worker automatically:
1. Waits for the server to be ready
2. Creates the `local-pool` work pool
3. Deploys all flows defined in `prefect.yaml`
4. Starts polling for flow runs

## Common commands

| Command | What it does |
|---------|-------------|
| `make up` | Start all services |
| `make down` | Stop all services |
| `make build` | Rebuild image after changing code, `prefect.yaml`, or `requirements.txt` |
| `make logs` | Tail all logs |
| `make logs-worker` | Tail worker logs only |
| `make deploy` | Redeploy all flows from `prefect.yaml` |
| `make test` | Run tests locally (no Docker needed) |
| `make shell` | Open a shell inside the worker container |
| `make clean` | Destroy containers + volumes (full reset) |

On Windows without `make`, run the `docker compose` commands directly, e.g.  
`docker compose up -d` instead of `make up`.

## Triggering a flow run

In the UI: Flows → pick a flow → Run.

From the CLI (inside the worker container):

```bash
make shell
prefect deployment run 'hello-world/hello-world-default'
prefect deployment run 'etl-example/etl-example-default'
```

## Adding a new flow

1. Create `flows/<domain>/<name>.py` — mirror `flows/examples/hello_world.py`
2. Add a deployment block to `prefect.yaml`
3. Run `make build` — bakes new code + updated `prefect.yaml` into the image, restarts worker, auto-deploys
4. Add tests in `tests/test_flows.py`

> `make deploy` runs **inside the container** and reads the baked-in `prefect.yaml`.
> If you changed `prefect.yaml` on the host, `make deploy` alone will use the old version.
> Always use `make build` when `prefect.yaml` changes.

### Minimal flow template

```python
from datetime import datetime
from prefect import flow, task, get_run_logger
from pydantic import BaseModel


class MyFlowInput(BaseModel):
    param: str = "default"


def _run_name() -> str:
    # Zero-arg — Prefect calls flow_run_name callables with NO arguments
    return f"my-flow-{datetime.now().strftime('%Y%m%d-%H%M%S')}"


@task(name="do-work", retries=3, retry_delay_seconds=10, log_prints=True)
def do_work(param: str) -> str:
    logger = get_run_logger()
    logger.info(f"Working with {param}")
    return f"done:{param}"


@flow(
    name="my-flow",
    flow_run_name=_run_name,     # zero-arg callable
    retries=2,
    retry_delay_seconds=10,
    timeout_seconds=300,
    log_prints=True,
)
def my_flow(input_: MyFlowInput = MyFlowInput()) -> str:
    try:
        return do_work(input_.param)
    except Exception:
        # add Slack alert here for production
        raise
```

### Minimal `prefect.yaml` entry

```yaml
  - name: my-flow-default
    entrypoint: flows/<domain>/<name>.py:my_flow
    parameters: {}
    work_pool:
      name: local-pool
      work_queue_name: default
      job_variables:
        image: prefect-platform:local
        name: "my-flow"          # static — {{ flow.name }} is NOT supported
        networks:
          - prefect-network
        env:
          PREFECT_API_URL: http://prefect-server:4200/api
          PYTHONPATH: /app
        auto_remove: true
    schedules: []
```

## Running tests

Tests run without Docker — `conftest.py` spins up an in-process Prefect environment.

```bash
# One-time: create venv and install deps
make setup

make test
```

On Windows (PowerShell), without `make`:

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
.\.venv\Scripts\python.exe -m pytest tests/ -v
```

## Architecture

```
prefect/
├── flows/                # @flow definitions, grouped by domain
│   └── examples/         # example flows to mirror
├── tasks/                # shared @task modules (reuse across flows)
├── tests/                # pytest tests — no live server needed
├── prefect.yaml          # all deployment definitions live here
├── docker-compose.yml    # postgres + prefect-server + prefect-worker
├── Dockerfile.worker     # worker image (prefect:3-latest + requirements)
└── entrypoint.sh         # worker startup: pool creation → deploy → start
```

### Three-layer rule

| Layer | Location | Responsibility |
|-------|----------|---------------|
| **Flow** | `flows/<domain>/` | Orchestration: wire tasks, set retries/timeout |
| **Task** | flow file or `tasks/common.py` | One verb, one side-effect, context-managed I/O |
| **Infra** | `docker-compose.yml` + `prefect.yaml` | Services and deployment config |

### Flow contract checklist

- [ ] Pydantic input model (`<Name>FlowInput`)
- [ ] `flow_run_name` zero-arg callable (`def _run_name() -> str`)
- [ ] `retries`, `retry_delay_seconds`, `timeout_seconds` on `@flow`
- [ ] `retries` on every `@task` that touches I/O
- [ ] `try / except … raise` — never swallow exceptions
- [ ] Entry in `prefect.yaml`
- [ ] Tests in `tests/`

### Git use
git init
git add .
git commit -m "Initial Prefect platform setup"
git add remote add origin https://github.com/kestutiss/prefect-platform.git
git branch -M main
git push -u origin main

### Flow lifecycle

 1. Work pool tells Prefect the infrastructure type

  When entrypoint.sh runs on startup:
  prefect work-pool create "local-pool" --type docker
  This registers local-pool in the Prefect server as a Docker pool. The type is stored server-side — that's how Prefect knows to use DockerWorker (visible in your log).

  ---
  2. Worker polls the server for flow runs

  The worker process inside prefect-worker container continuously asks the server:

  ▎ "Any flow runs queued for local-pool?"

  When you trigger a run from the UI, the server queues it. The worker picks it up:
  Worker 'DockerWorker 1505811a...' submitting flow run '59add183...'

  ---
  3. job_variables tell the worker HOW to launch the container

  From your prefect.yaml:
  job_variables:
    image: prefect-platform:local      # which image to run
    networks:
      - prefect-network                # so container can reach prefect-server
    env:
      PREFECT_API_URL: http://prefect-server:4200/api
      PYTHONPATH: /app
    auto_remove: true                  # container deleted after run

  The worker reads these, then calls the host Docker daemon (via the mounted /var/run/docker.sock) to start a container — equivalent to:
  docker run --rm \
    --network prefect-network \
    -e PREFECT_API_URL=http://prefect-server:4200/api \
    -e PYTHONPATH=/app \
    prefect-platform:local \
    prefect execute flow-run 59add183-8af7-4ecb-9ec0-4aea5958ca8f

  ---
  4. Flow-run container executes the flow

  The new container:
  1. Connects back to prefect-server:4200 to fetch the flow run details
  2. Imports and runs the flow function (etl_example_flow)
  3. Streams all logs back to the server in real time — that's why you see them in the UI
  4. Exits when done → container is auto-removed

  Beginning flow run 'etl-20260607-071702'   ← container started, flow running
  ...
  Finished in state Completed()              ← container exits, removed

  ---
  Full picture

  prefect-server
      │  queues flow run
      ▼
  prefect-worker container
      │  reads job_variables from prefect.yaml
      │  calls /var/run/docker.sock
      ▼
  Docker daemon (host)
      │  docker run prefect-platform:local
      ▼
  flow-run container (temporary)
      │  fetches run config from prefect-server
      │  runs etl_example_flow()
      │  streams logs → prefect-server
      └─ exits + auto-removed

  The worker never runs the flow itself — it only schedules the container. The container is the actual executor.