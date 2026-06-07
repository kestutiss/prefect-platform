"""Hello world flow — the simplest possible Prefect 3.x example.

Mirror this file when adding a new flow:
  1. Define a Pydantic input model.
  2. Add @task functions (retries on anything that touches I/O).
  3. Wire them in @flow with retries + timeout_seconds.
  4. Add a deployment entry to prefect.yaml.
"""
from datetime import datetime, timedelta

from prefect import flow, get_run_logger, task, unmapped
from prefect.tasks import task_input_hash
from pydantic import BaseModel


class HelloFlowInput(BaseModel):
    names: list[str] = ["World", "Prefect"]
    greeting: str = "Hello"


def _run_name() -> str:
    return f"hello-world-{datetime.now().strftime('%Y%m%d-%H%M%S')}"


@task(
    name="greet",
    retries=1,
    retry_delay_seconds=5,
    cache_key_fn=task_input_hash,
    cache_expiration=timedelta(minutes=10),
)
def greet(name: str, greeting: str) -> str:
    """Return a greeting message for the given name."""
    logger = get_run_logger()
    message = f"{greeting}, {name}!"
    logger.info(message)
    return message


@task(name="collect-greetings")
def collect_greetings(greetings: list[str]) -> str:
    """Join individual greetings into a single newline-separated string."""
    return "\n".join(greetings)


@flow(
    name="hello-world",
    flow_run_name=_run_name,
    retries=2,
    retry_delay_seconds=10,
    timeout_seconds=300,
    log_prints=True,
)
def hello_world_flow(input_: HelloFlowInput = HelloFlowInput()) -> str:
    """Greet each name in the input list in parallel, then collect results."""
    try:
        # .map() runs one task instance per name concurrently
        futures = greet.map(input_.names, unmapped(input_.greeting))
        result = collect_greetings(futures)
        print(f"Completed greetings:\n{result}")
        return result
    except Exception:
        # Add a Slack / PagerDuty alert here for production deployments
        raise
