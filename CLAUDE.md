# Prefect Platform — Claude Code context

Local Prefect 3.x platform. Docker Compose runs postgres + prefect-server + prefect-worker.

## Project layout

```
flows/          @flow definitions, grouped by domain subfolder
tasks/          shared @task modules imported by multiple flows
tests/          pytest — uses prefect_test_harness, no live server needed
prefect.yaml    deployment registry — every flow must have an entry here
```

## Infrastructure — Docker work pool

Each flow run spawns a **fresh Docker container** (not a subprocess inside the worker):

```
prefect-worker container
    └── calls host Docker daemon via /var/run/docker.sock
            └── new container: prefect-platform:local
                    └── runs the flow, streams logs to server, removed on completion
```

Key facts:
- Single image `prefect-platform:local` is used for both the worker and flow-run containers
- All containers join `prefect-network` (stable name) so flow containers can resolve `prefect-server`
- `prefect-docker` package **must** be in `requirements.txt` — without it the worker cannot start the Docker work pool worker type
- Flow code is **baked into the image** — not volume mounted. Code changes require `make build`

## Adding a flow (use /prefect-flow-builder skill)

1. `flows/<domain>/<name>.py` — mirror `flows/examples/hello_world.py`
2. Add deployment block to `prefect.yaml` (include full `job_variables` — see template below)
3. `make build` — rebuilds image with new code, restarts worker, auto-deploys
4. `tests/test_flows.py` — add test class

## Required @flow decorator fields

```python
@flow(
    name="<snake-case>",
    flow_run_name=_run_name,     # MUST be a zero-arg callable — Prefect calls it with no args
    retries=2,
    retry_delay_seconds=10,
    timeout_seconds=<n>,         # REQUIRED — prevents runaway flow-run containers
    log_prints=True,
)
def my_flow(input_: MyFlowInput | None = None) -> ResultType:
    if input_ is None:
        input_ = MyFlowInput()
    try:
        ...
    except Exception:
        # add Slack / PagerDuty alert here for production
        raise
```

`_run_name` pattern:
```python
def _run_name() -> str:
    return f"my-flow-{datetime.now().strftime('%Y%m%d-%H%M%S')}"
```

## Required @task fields

- `retries=3, retry_delay_seconds=5` on every task that touches I/O (file, network, DB)
- Pure transforms (no I/O) do NOT need retries
- Never swallow exceptions — always re-raise after handling

## prefect.yaml deployment entry template

Every new deployment must include the full `job_variables` block for the Docker work pool:

```yaml
  - name: <flow-name>-default
    version: "1.0"
    tags: []
    entrypoint: flows/<domain>/<name>.py:<flow_function>
    parameters: {}
    work_pool:
      name: local-pool
      work_queue_name: default
      job_variables:
        image: prefect-platform:local
        networks:
          - prefect-network
        env:
          PREFECT_API_URL: http://prefect-server:4200/api
          PYTHONPATH: /app
        auto_remove: true
    schedules: []
```

## Dev commands

```
make up              # start all services (first time builds image automatically)
make build           # rebuild image after code/requirements changes, restart worker
make logs-worker     # watch worker logs
make deploy          # redeploy from prefect.yaml without rebuilding
make test            # run pytest locally (requires venv — see make setup)
make setup           # create .venv and install requirements (run once for local tests)
make shell           # bash inside worker container
make clean           # full reset (removes volumes)
```

Prefect UI: http://localhost:4200

## Testing locally (no Docker needed)

```bash
make setup    # once: creates .venv and installs requirements.txt
make test     # runs pytest with prefect_test_harness (in-process, no server)
```

Tests call flows directly as functions. Use `tmp_path` for any file output paths.
