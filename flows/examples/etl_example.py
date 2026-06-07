"""ETL example flow — extract → transform → load pattern.

Demonstrates:
- Pydantic input model with defaults
- Three-task ETL structure with proper retries
- Schema validation after transform
- Idempotent file output (overwrite-safe)
"""
from datetime import datetime
from pathlib import Path

import pandas as pd
from prefect import flow, get_run_logger, task
from pydantic import BaseModel


class ETLFlowInput(BaseModel):
    source: str = "sample"
    batch_size: int = 100
    output_path: str = "/tmp/etl_output.csv"


def _run_name() -> str:
    return f"etl-{datetime.now().strftime('%Y%m%d-%H%M%S')}"


@task(
    name="extract-data",
    retries=3,
    retry_delay_seconds=10,
    log_prints=True,
)
def extract_data(input_: ETLFlowInput) -> pd.DataFrame:
    """Extract raw data from source (simulated with generated data)."""
    logger = get_run_logger()
    logger.info(f"Extracting {input_.batch_size} records from '{input_.source}'")

    df = pd.DataFrame(
        {
            "id": range(input_.batch_size),
            "value": [i * 2.5 for i in range(input_.batch_size)],
            "category": [f"cat_{i % 5}" for i in range(input_.batch_size)],
            "extracted_at": datetime.now().isoformat(),
        }
    )
    logger.info(f"Extracted {len(df)} records")
    return df


@task(name="transform-data", log_prints=True)
def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    """Apply business logic: normalize values, uppercase categories, validate schema."""
    logger = get_run_logger()

    df = df.copy()
    mean, std = df["value"].mean(), df["value"].std()
    df["value_normalized"] = (df["value"] - mean) / std
    df["category_upper"] = df["category"].str.upper()
    df["transformed_at"] = datetime.now().isoformat()

    required_cols = {"id", "value", "category", "value_normalized"}
    if missing := required_cols - set(df.columns):
        raise ValueError(f"Schema drift — missing columns after transform: {missing}")

    logger.info(f"Transformed {len(df)} records")
    return df


@task(
    name="load-data",
    retries=2,
    retry_delay_seconds=5,
    log_prints=True,
)
def load_data(df: pd.DataFrame, output_path: str) -> int:
    """Write transformed data to destination (idempotent CSV overwrite)."""
    logger = get_run_logger()
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False)
    logger.info(f"Loaded {len(df)} records → {output_path}")
    return len(df)


@flow(
    name="etl-example",
    flow_run_name=_run_name,
    retries=2,
    retry_delay_seconds=30,
    timeout_seconds=600,
    log_prints=True,
)
def etl_example_flow(input_: ETLFlowInput = ETLFlowInput()) -> int:
    """Run extract → transform → load pipeline and return record count."""
    print(f"Starting ETL: source={input_.source!r} → {input_.output_path!r}")

    try:
        raw_df = extract_data(input_)
        transformed_df = transform_data(raw_df)
        record_count = load_data(transformed_df, input_.output_path)
        print(f"ETL complete: {record_count} records")
        return record_count
    except Exception:
        # Add a Slack / PagerDuty alert here for production deployments
        raise
