import os
from datetime import datetime, timezone
from typing import Optional

from prefect import flow, get_run_logger, task
from pydantic import BaseModel, Field
from pymongo import ReturnDocument

from tasks.mongo_client import get_mongo_client


class DeactivateExpiredCouponInput(BaseModel):
    # Reads MONGO_URI from env so Docker flow containers reach mongo:27017
    # while local runs and tests fall back to localhost.
    mongo_uri: str = Field(
        default_factory=lambda: os.getenv("MONGO_URI", "mongodb://localhost:27017")
    )
    database: str = "account"
    collection: str = "coupons"
    dry_run: bool = False


def _run_name() -> str:
    return f"deactivate-expired-coupon-{datetime.now().strftime('%Y%m%d-%H%M%S')}"


@task(
    name="find-most-expired-coupon",
    retries=3,
    retry_delay_seconds=5,
    log_prints=True,
)
def find_most_expired_coupon(input_: DeactivateExpiredCouponInput) -> Optional[dict]:
    """Query coupons for the single most-expired non-deactivated document."""
    logger = get_run_logger()
    with get_mongo_client(input_.mongo_uri) as client:
        coll = client[input_.database][input_.collection]
        coupon = coll.find_one(
            {
                "status": {"$ne": "DEACTIVATED"},
                "validTo": {"$lt": datetime.now(timezone.utc).replace(tzinfo=None)},
            },
            sort=[("validTo", 1)],  # ascending: earliest validTo = most expired
        )
    if coupon is None:
        logger.info("No expired non-deactivated coupons found")
        return None
    logger.info(
        f"Found most expired coupon: couponId={coupon['couponId']!r}, validTo={coupon['validTo']}"
    )
    return coupon


@task(name="prepare-deactivation", log_prints=True)
def prepare_deactivation(coupon: dict) -> dict:
    """Build the MongoDB update payload for deactivation (pure transform, no I/O)."""
    logger = get_run_logger()
    logger.info(f"Preparing deactivation for couponId={coupon['couponId']!r}")
    return {"$set": {"status": "DEACTIVATED", "deactivatedAt": datetime.now(timezone.utc).replace(tzinfo=None)}}


@task(
    name="deactivate-coupon",
    retries=3,
    retry_delay_seconds=5,
    log_prints=True,
)
def deactivate_coupon(
    coupon: dict,
    update: dict,
    input_: DeactivateExpiredCouponInput,
) -> str:
    """Apply the deactivation update atomically; return couponId."""
    logger = get_run_logger()
    coupon_id = coupon["couponId"]
    if input_.dry_run:
        logger.info(f"[dry_run] Would deactivate couponId={coupon_id!r}")
        return coupon_id
    with get_mongo_client(input_.mongo_uri) as client:
        coll = client[input_.database][input_.collection]
        result = coll.find_one_and_update(
            {"couponId": coupon_id},
            update,
            upsert=False,
            return_document=ReturnDocument.AFTER,
        )
    if result is None:
        # Document deleted between find and update — treat as a hard error so the
        # flow retries rather than silently completing with a missing record.
        raise ValueError(
            f"Coupon {coupon_id!r} vanished during update — possible concurrent deletion"
        )
    logger.info(
        f"Deactivated couponId={result['couponId']!r}, deactivatedAt={result['deactivatedAt']}"
    )
    return coupon_id


@flow(
    name="deactivate-expired-coupon",
    flow_run_name=_run_name,
    retries=2,
    retry_delay_seconds=10,
    timeout_seconds=120,
    log_prints=True,
)
def deactivate_expired_coupon_flow(
    input_: DeactivateExpiredCouponInput = DeactivateExpiredCouponInput(),
) -> Optional[str]:
    """Find the most expired non-deactivated coupon and deactivate it.

    Returns the couponId that was deactivated, or None if no eligible coupon exists.
    """
    try:
        coupon = find_most_expired_coupon(input_)
        if coupon is None:
            return None
        update = prepare_deactivation(coupon)
        return deactivate_coupon(coupon, update, input_)
    except Exception:
        # Add Slack / PagerDuty alert here for production
        raise
