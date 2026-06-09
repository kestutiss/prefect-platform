"""Seed the coupons collection with test data for local development.

Inserts 5 coupons that cover all states the deactivate-expired-coupon flow
can encounter. Safe to re-run — uses upsert on couponId.

Usage:
    make seed
    MONGO_URI=mongodb://... make seed

State after seeding:
    SEED-SUMMER-50      ACTIVE      expired 45 days ago  ← most expired, deactivated first
    SEED-WINTER-20      ACTIVE      expired  7 days ago
    SEED-FLASH-10       ACTIVE      expired  1 day  ago
    SEED-SPRING-15      ACTIVE      valid for 30 more days  (never selected)
    SEED-BLACK-FRIDAY   DEACTIVATED already done           (skipped by flow)
"""
import os
from datetime import datetime, timedelta, timezone

from pymongo import MongoClient

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
DATABASE = "account"
COLLECTION = "coupons"


def _utcnow() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _build_coupons() -> list[dict]:
    now = _utcnow()
    return [
        {
            "couponId": "SEED-SUMMER-50",
            "name": "Summer Sale 50% off",
            "status": "ACTIVE",
            "validFrom": now - timedelta(days=120),
            "validTo": now - timedelta(days=45),
            "createdAt": now - timedelta(days=120),
        },
        {
            "couponId": "SEED-WINTER-20",
            "name": "Winter 20% discount",
            "status": "ACTIVE",
            "validFrom": now - timedelta(days=40),
            "validTo": now - timedelta(days=7),
            "createdAt": now - timedelta(days=40),
        },
        {
            "couponId": "SEED-FLASH-10",
            "name": "Flash sale 10% off",
            "status": "ACTIVE",
            "validFrom": now - timedelta(days=3),
            "validTo": now - timedelta(days=1),
            "createdAt": now - timedelta(days=3),
        },
        {
            "couponId": "SEED-SPRING-15",
            "name": "Spring promo 15% off",
            "status": "ACTIVE",
            "validFrom": now - timedelta(days=5),
            "validTo": now + timedelta(days=30),
            "createdAt": now - timedelta(days=5),
        },
        {
            "couponId": "SEED-BLACK-FRIDAY",
            "name": "Black Friday 30% off",
            "status": "DEACTIVATED",
            "validFrom": now - timedelta(days=220),
            "validTo": now - timedelta(days=180),
            "createdAt": now - timedelta(days=220),
            "deactivatedAt": now - timedelta(days=10),
        },
    ]


def main() -> None:
    print(f"Connecting to {MONGO_URI!r}, database={DATABASE!r}")
    client = MongoClient(MONGO_URI)
    try:
        coll = client[DATABASE][COLLECTION]
        coupons = _build_coupons()
        for coupon in coupons:
            coll.update_one({"couponId": coupon["couponId"]}, {"$set": coupon}, upsert=True)

        print(f"\nSeeded {len(coupons)} coupons into {DATABASE}.{COLLECTION}:\n")
        header = f"  {'couponId':<25}  {'status':<13}  validTo"
        print(header)
        print("  " + "-" * (len(header) - 2))
        for c in coupons:
            print(
                f"  {c['couponId']:<25}  {c['status']:<13}  {c['validTo'].strftime('%Y-%m-%d')}"
            )
        print()
    finally:
        client.close()


if __name__ == "__main__":
    main()
