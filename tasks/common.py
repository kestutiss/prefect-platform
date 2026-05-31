"""Shared, reusable Prefect tasks for common I/O operations.

Import these into any flow instead of re-implementing file/JSON handling.
"""
import json
from datetime import timedelta
from pathlib import Path

from prefect import get_run_logger, task
from prefect.tasks import task_input_hash


@task(
    name="write-json-file",
    retries=2,
    retry_delay_seconds=5,
    log_prints=True,
)
def write_json_file(data: dict, path: str) -> str:
    """Write a dict to a JSON file, creating parent directories as needed."""
    logger = get_run_logger()
    output = Path(path)
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("w") as f:
        json.dump(data, f, indent=2, default=str)
    logger.info(f"Wrote {len(data)} keys → {path}")
    return path


@task(
    name="read-json-file",
    retries=2,
    retry_delay_seconds=5,
    cache_key_fn=task_input_hash,
    cache_expiration=timedelta(minutes=5),
    log_prints=True,
)
def read_json_file(path: str) -> dict:
    """Read a JSON file and return its contents as a dict."""
    logger = get_run_logger()
    with open(path) as f:
        data = json.load(f)
    logger.info(f"Read {len(data)} keys ← {path}")
    return data
