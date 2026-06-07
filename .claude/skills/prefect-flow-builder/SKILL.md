---
name: prefect-flow-builder
description: >
  Build a new, production-ready Prefect 3.x data flow in this repo (extract →
  transform → load), wiring the flow decorator, tasks, client usage, pydantic
  input contract, deployment YAML, and tests. Use this skill WHENEVER the user
  asks to "add a flow", "build a pipeline", "create an ETL", "unload/sync/enrich/
  ban/monitor <something>", or otherwise touch flows/ or tasks/ — even if they
  don't say the word "Prefect". Also use it when reviewing or fixing an existing
  flow's decorator settings, retries, timeouts, or YAML wiring.
---

# Building a Prefect flow in this repo

This repo is a local Prefect 3.x platform backed by a **Docker work pool** — each
flow run executes in its own container (`prefect-platform:local`). A new flow
requires changes in four places: the flow file, tasks (if shared), `prefect.yaml`,
and tests. Do all of it or the deployment will not register.

> Read this whole file before writing code. Then open `flows/examples/hello_world.py`
> and mirror its shape exactly — decorator fields, input model, try/except, run name.

---

## 0. Before writing anything — orient

1. `ls flows/` — find the domain subfolder closest to the task, or create one.
   Open the most similar existing flow and mirror its shape.
2. `ls tasks/` — check if a shared task already exists before writing a new one.
   Shared tasks live in `tasks/common.py`. Simple flows can define tasks inline.
3. `cat requirements.txt` — confirm the packages the flow needs are already listed.
   Add missing ones; they are baked into the image on `make build`.
4. `cat prefect.yaml` — check existing deployment names to avoid collisions.

If anything is ambiguous (source, output path, schedule, batch size), ask the user
before generating code.

---

## 1. The three layers (never collapse them)

| Layer | Location | Responsibility | Forbidden |
|-------|----------|----------------|-----------|
| **Flow** `@flow` | `flows/<domain>/<name>.py` | Orchestration only: wire extract→transform→load, set retries/timeout, top-level try/except. | Business logic, inline SQL, raw I/O. |
| **Task** `@task` | same file (simple) or `tasks/common.py` (shared) | One verb, one side-effect. | Multiple sources/destinations; business logic mixed with I/O. |
| **Infra** | `docker-compose.yml` + `prefect.yaml` | Services and deployment config. | Hardcoded credentials, env-specific values. |

One `@flow` entrypoint per file (the unit of deployment).

---

## 2. Flow contract (copy this shape exactly)

```python
from datetime import datetime
from prefect import flow, task, get_run_logger
from pydantic import BaseModel


class MyFlowInput(BaseModel):
    param: str = "default"


def _run_name() -> str:
    # MUST be zero-arg — Prefect calls flow_run_name callables with NO arguments.
    # Adding any parameter raises TypeError at runtime.
    return f"my-flow-{datetime.now().strftime('%Y%m%d-%H%M%S')}"


@flow(
    name="my-flow",
    flow_run_name=_run_name,        # zero-arg callable
    retries=2,
    retry_delay_seconds=10,
    timeout_seconds=300,            # REQUIRED — prevents runaway containers
    log_prints=True,
)
def my_flow(input_: MyFlowInput = MyFlowInput()) -> ResultType:
    try:
        raw   = extract_something(input_)
        clean = transform_something(raw)
        count = load_somewhere(clean, input_)
        return count
    except Exception:
        # Add Slack / PagerDuty alert here for production
        raise
```

Hard requirements for every new flow:
- **Exactly one Pydantic input model** defined in the same file. No bare positional
  primitives on the flow function.
- `flow_run_name` must be a **zero-arg callable** (`def _run_name() -> str`).
  Use `input_: MyFlowInput = MyFlowInput()` as the default (never `= None`).
- `retries`, `retry_delay_seconds`, and `timeout_seconds` — all three, always.
- Top-level `try/except` that re-raises — never swallow.

---

## 3. Task contract

```python
@task(
    name="extract-something",
    retries=3,               # required for any task that touches I/O
    retry_delay_seconds=5,
    log_prints=True,
)
def extract_something(input_: MyFlowInput) -> pd.DataFrame:
    """Extract raw data from source."""
    logger = get_run_logger()
    logger.info(f"Extracting from {input_.param}")
    ...
    return df
```

- Any task that performs file, network, or database I/O: `retries=3, retry_delay_seconds=5`.
- Pure transforms (no I/O): no retries needed.
- Add result caching with `cache_key_fn=task_input_hash` when the task is
  idempotent and expensive (e.g. HTTP fetches, DB queries).
- Never swallow exceptions inside a task — a `Completed` state after a silent
  failure corrupts the run history.

---

## 4. ETL data-integrity rules

- **Idempotency**: file writes use overwrite mode; DB writes use upsert/merge with
  an explicit dedup key. Append-only into a target that can't tolerate duplicates
  is a bug.
- **Schema drift**: add explicit column-presence checks in the transform step;
  raise `ValueError` early rather than silently producing wrong output.
- **No data exposure**: never log PII, tokens, or raw customer identifiers.

---

## 5. Adding the deployment to prefect.yaml

A flow that isn't in `prefect.yaml` will not appear in the UI and cannot be run.
Copy this block and fill in the three fields marked with `<>`:

```yaml
  - name: <flow-name>-default
    version: "1.0"
    tags: []
    description: "<one-line description>"
    entrypoint: flows/<domain>/<name>.py:<flow_function>
    parameters: {}
    work_pool:
      name: local-pool
      work_queue_name: default
      job_variables:
        image: prefect-platform:local      # image with flow code baked in
        name: "<flow-name>"                # static string — see note below
        networks:
          - prefect-network                # lets container reach prefect-server
        env:
          PREFECT_API_URL: http://prefect-server:4200/api
          PYTHONPATH: /app
        auto_remove: true
    schedules: []
```

Notes:
- `retries` and `timeout_seconds` live on the `@flow` decorator, not the YAML.
- `job_variables` is **required** for the Docker work pool — omitting it means the
  flow-run container cannot reach the Prefect server and will fail immediately.
- `name` must be a **static string** — `{{ flow.name }}` looks valid but is NOT in
  Prefect's runtime template context (only `flow_run.*` fields are available, and
  none contain a human-readable flow name). Prefect warns silently and falls back to
  a random Docker name if an unsupported placeholder is used.
- `prefect.yaml` is baked into the image. After editing it, **always run `make build`**
  — `make deploy` runs inside the container and reads the old baked-in file.

---

## 6. Dependencies

If the new flow needs a package not already in `requirements.txt`:

1. Add it to `requirements.txt`.
2. Run `make build` — rebuilds the image so the package is available in both the
   worker and flow-run containers.
3. Do NOT import optional packages at module level; the import must succeed in the
   image or every deployment will fail to load.

Critical packages already required by the platform:
- `prefect>=3.0.0` — core
- `prefect-docker>=0.6.0` — **must stay** — without it the Docker work pool worker
  type cannot start

---

## 7. Tests

Add a test class to `tests/test_flows.py`. The `prefect_test_harness` fixture in
`conftest.py` is session-scoped and automatic — no extra setup needed.

```python
from flows.<domain>.<name> import MyFlowInput, my_flow

class TestMyFlow:
    def test_default_run(self, tmp_path):
        input_ = MyFlowInput(output_path=str(tmp_path / "out.csv"))
        result = my_flow(input_=input_)
        assert result == <expected>

    def test_custom_params(self):
        input_ = MyFlowInput(param="custom")
        result = my_flow(input_=input_)
        assert "custom" in result
```

Rules:
- Call the flow function directly — no server or Docker needed.
- Use `tmp_path` for any file output paths so tests don't collide.
- Run with `make test` (requires `make setup` once to create the venv).
- All tests must pass before marking the task done.

---

## 8. Self-review before handing back

- [ ] Input model defined (`<Name>FlowInput`), no bare positional primitives.
- [ ] `_run_name()` is zero-arg — no parameters.
- [ ] `@flow` has `retries`, `retry_delay_seconds`, and `timeout_seconds`.
- [ ] All I/O tasks have `retries=3, retry_delay_seconds=5`.
- [ ] No exception swallowed into a false `Completed` state.
- [ ] Deployment block added to `prefect.yaml` with full `job_variables`.
- [ ] New packages added to `requirements.txt` if needed.
- [ ] Tests added to `tests/test_flows.py`; `make test` passes.
- [ ] No PII / tokens in logs or exception messages.

---

## Appendix — what to include in the prompt

1. **Goal**: one sentence on what the flow does.
2. **Source**: where the data comes from (file path, API endpoint, DB table) and
   expected volume (drives chunking decisions).
3. **Transform**: what shape/filtering/aggregation is needed.
4. **Destination + write semantics**: output path or table, overwrite vs. append,
   idempotency key.
5. **Schedule**: interval or cron in UTC (or "manual only").
6. **Parameters**: which fields go on the Pydantic input model.
7. **New dependencies**: any packages not already in `requirements.txt`.
