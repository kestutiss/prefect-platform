"""Tests for the deactivate-expired-coupon flow.

Requires Docker — testcontainers spins up a real MongoDB 7 instance.
Run with: make test  (or pytest tests/test_deactivate_expired_coupon.py -v)
"""
from datetime import datetime, timedelta, timezone

import pytest

from flows.mongodb.deactivate_expired_coupon import (
    DeactivateExpiredCouponInput,
    deactivate_expired_coupon_flow,
)


def _coupon(coupon_id: str, days_offset: int, status: str = "ACTIVE") -> dict:
    """Build a coupon document. Negative days_offset puts validTo in the past."""
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    return {
        "couponId": coupon_id,
        "name": f"Test coupon {coupon_id}",
        "status": status,
        "validFrom": now - timedelta(days=60),
        "validTo": now + timedelta(days=days_offset),
        "createdAt": now - timedelta(days=60),
    }


def _input(mongodb_container) -> DeactivateExpiredCouponInput:
    return DeactivateExpiredCouponInput(
        mongo_uri=mongodb_container.get_connection_url(),
        database="account",
        collection="coupons",
    )


class TestDeactivateExpiredCouponFlow:

    def test_most_expired_is_selected(self, coupons_collection, mongodb_container):
        coupons_collection.insert_many([
            _coupon("C001", -10),   # expired 10 days ago — should be selected
            _coupon("C002", -5),
            _coupon("C003", -1),
        ])
        result = deactivate_expired_coupon_flow(input_=_input(mongodb_container))
        assert result == "C001"
        doc = coupons_collection.find_one({"couponId": "C001"})
        assert doc["status"] == "DEACTIVATED"
        assert doc["deactivatedAt"] is not None

    def test_no_expired_coupons_returns_none(self, coupons_collection, mongodb_container):
        coupons_collection.insert_many([
            _coupon("C004", 10),
            _coupon("C005", 30),
        ])
        result = deactivate_expired_coupon_flow(input_=_input(mongodb_container))
        assert result is None

    def test_active_coupon_not_touched(self, coupons_collection, mongodb_container):
        coupons_collection.insert_many([
            _coupon("C006", 30),    # active — must not be touched
            _coupon("C007", -3),    # expired — will be deactivated
        ])
        deactivate_expired_coupon_flow(input_=_input(mongodb_container))
        doc = coupons_collection.find_one({"couponId": "C006"})
        assert doc["status"] == "ACTIVE"

    def test_already_deactivated_skipped(self, coupons_collection, mongodb_container):
        coupons_collection.insert_one(_coupon("C008", -5, status="DEACTIVATED"))
        result = deactivate_expired_coupon_flow(input_=_input(mongodb_container))
        assert result is None

    def test_deactivated_at_timestamp_is_recent(self, coupons_collection, mongodb_container):
        before = datetime.now(timezone.utc).replace(tzinfo=None)
        coupons_collection.insert_one(_coupon("C009", -2))
        deactivate_expired_coupon_flow(input_=_input(mongodb_container))
        doc = coupons_collection.find_one({"couponId": "C009"})
        assert doc["deactivatedAt"] is not None
        assert doc["deactivatedAt"] >= before

    def test_dry_run_does_not_modify(self, coupons_collection, mongodb_container):
        coupons_collection.insert_one(_coupon("C010", -5))
        input_ = DeactivateExpiredCouponInput(
            mongo_uri=mongodb_container.get_connection_url(),
            database="account",
            collection="coupons",
            dry_run=True,
        )
        result = deactivate_expired_coupon_flow(input_=input_)
        assert result == "C010"
        doc = coupons_collection.find_one({"couponId": "C010"})
        assert doc["status"] == "ACTIVE"

    def test_empty_collection_returns_none(self, coupons_collection, mongodb_container):
        result = deactivate_expired_coupon_flow(input_=_input(mongodb_container))
        assert result is None

    @pytest.mark.parametrize("count", [1, 5])
    def test_only_one_coupon_deactivated_per_run(
        self, coupons_collection, mongodb_container, count
    ):
        coupons_collection.insert_many([_coupon(f"D{i:03}", -i - 1) for i in range(count)])
        deactivate_expired_coupon_flow(input_=_input(mongodb_container))
        deactivated = list(coupons_collection.find({"status": "DEACTIVATED"}))
        assert len(deactivated) == 1
