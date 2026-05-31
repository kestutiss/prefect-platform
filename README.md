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
| `make build` | Rebuild worker image (after changing `requirements.txt`) |
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
3. Redeploy: `make deploy`
4. Add tests in `tests/test_flows.py`

### Minimal flow template

```python
from datetime import datetime
from prefect import flow, task, get_run_logger
from pydantic import BaseModel


class MyFlowInput(BaseModel):
    param: str = "default"


def _run_name(input_: MyFlowInput) -> str:
    return f"my-flow-{datetime.now().strftime('%Y%m%d-%H%M%S')}"


@task(name="do-work", retries=3, retry_delay_seconds=10, log_prints=True)
def do_work(param: str) -> str:
    logger = get_run_logger()
    logger.info(f"Working with {param}")
    return f"done:{param}"


@flow(
    name="my-flow",
    flow_run_name=_run_name,
    retries=2,
    retry_delay_seconds=10,
    timeout_seconds=300,
    log_prints=True,
)
def my_flow(input_: MyFlowInput | None = None) -> str:
    if input_ is None:
        input_ = MyFlowInput()
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
- [ ] `flow_run_name` callable
- [ ] `retries`, `retry_delay_seconds`, `timeout_seconds` on `@flow`
- [ ] `retries` on every `@task` that touches I/O
- [ ] `try / except … raise` — never swallow exceptions
- [ ] Entry in `prefect.yaml`
- [ ] Tests in `tests/`
