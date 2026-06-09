"""Create (or upsert) next week's coupon in account.coupons.

The coupon data isn't a user input — it's derived from the calendar:
the coupon name comes from WORDS indexed by next week's ISO number, and the
validity period spans that whole ISO week. Running the flow multiple times
within the same week is idempotent (same couponId, upsert).

Layers:
  tasks/coupon_factory.py   → pure helpers (name + document builder)
  build_coupon_document     → task wrapper around the builder (Prefect lineage)
  upsert_coupon             → I/O task: idempotent write to MongoDB
"""
import os
from datetime import datetime

from prefect import flow, get_run_logger, task
from pydantic import BaseModel, Field

from tasks.coupon_factory import build_weekly_coupon
from tasks.mongo_client import get_mongo_client


class CreateCouponInput(BaseModel):
    """Connection config only — coupon data is generated, not provided."""
    mongo_uri: str = Field(
        default_factory=lambda: os.getenv("MONGO_URI", "mongodb://localhost:27017")
    )
    database: str = "account"
    collection: str = "coupons"


def _run_name() -> str:
    return f"create-coupon-{datetime.now().strftime('%Y%m%d-%H%M%S')}"


@task(name="build-coupon-document", log_prints=True)
def build_coupon_document() -> dict:
    """Pure transform: assemble next week's coupon document via coupon_factory."""
    logger = get_run_logger()
    doc = build_weekly_coupon()
    logger.info(f"Built coupon: couponId={doc['couponId']!r}, name={doc['name']!r}")
    return doc


@task(
    name="upsert-coupon",
    retries=3,
    retry_delay_seconds=5,
    log_prints=True,
)
def upsert_coupon(coupon: dict, input_: CreateCouponInput) -> str:
    """Upsert by couponId. Preserves createdAt; clears any stale deactivatedAt."""
    logger = get_run_logger()
    coupon_id = coupon["couponId"]
    created_at = coupon.pop("createdAt")
    with get_mongo_client(input_.mongo_uri) as client:
        coll = client[input_.database][input_.collection]
        coll.update_one(
            {"couponId": coupon_id},
            {
                "$set": coupon,
                "$setOnInsert": {"createdAt": created_at},
                "$unset": {"deactivatedAt": ""},
            },
            upsert=True,
        )
    logger.info(f"Upserted couponId={coupon_id!r}, status={coupon['status']!r}")
    return coupon_id


@flow(
    name="create-coupon",
    flow_run_name=_run_name,
    retries=2,
    retry_delay_seconds=10,
    timeout_seconds=120,
    log_prints=True,
)
def create_coupon_flow(input_: CreateCouponInput = CreateCouponInput()) -> str:
    """Generate and upsert next week's coupon. Returns the couponId."""
    try:
        coupon = build_coupon_document()
        return upsert_coupon(coupon, input_)
    except Exception:
        # Add Slack / PagerDuty alert here for production
        raise
