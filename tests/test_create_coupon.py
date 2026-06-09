"""Tests for the create-coupon flow.

Requires Docker — testcontainers spins up a real MongoDB 7 instance.
Run with: make test  (or pytest tests/test_create_coupon.py -v)
"""
import time
from datetime import datetime, timedelta, timezone

from flows.mongodb.create_coupon import (
    CreateCouponInput,
    create_coupon_flow,
)
from tasks.coupon_factory import build_weekly_coupon


def _utcnow() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _input(mongodb_container) -> CreateCouponInput:
    return CreateCouponInput(
        mongo_uri=mongodb_container.get_connection_url(),
        database="account",
        collection="coupons",
    )


class TestCreateCouponFlow:

    def test_inserts_into_empty_collection(self, coupons_collection, mongodb_container):
        # "empty result" edge case
        assert coupons_collection.count_documents({}) == 0
        result = create_coupon_flow(input_=_input(mongodb_container))
        assert result is not None
        assert coupons_collection.count_documents({}) == 1

    def test_new_coupon_has_status_inactive(self, coupons_collection, mongodb_container):
        # happy path — flow creates a coupon that exists with status INACTIVE
        coupon_id = create_coupon_flow(input_=_input(mongodb_container))
        doc = coupons_collection.find_one({"couponId": coupon_id})
        assert doc is not None
        assert doc["status"] == "INACTIVE"

    def test_coupon_matches_factory_output(self, coupons_collection, mongodb_container):
        # the persisted document matches what coupon_factory builds
        expected = build_weekly_coupon()
        coupon_id = create_coupon_flow(input_=_input(mongodb_container))
        assert coupon_id == expected["couponId"]
        doc = coupons_collection.find_one({"couponId": coupon_id})
        assert doc["name"] == expected["name"]
        assert doc["validFrom"] == expected["validFrom"]
        assert doc["validTo"] == expected["validTo"]

    def test_coupon_id_has_weekly_format(self, coupons_collection, mongodb_container):
        coupon_id = create_coupon_flow(input_=_input(mongodb_container))
        # WEEK-YYYY-Www
        assert coupon_id.startswith("WEEK-")
        assert "-W" in coupon_id

    def test_document_has_all_expected_fields(self, coupons_collection, mongodb_container):
        coupon_id = create_coupon_flow(input_=_input(mongodb_container))
        doc = coupons_collection.find_one({"couponId": coupon_id})
        for field in ("couponId", "name", "status", "validFrom", "validTo", "createdAt"):
            assert field in doc, f"Missing field: {field}"
        assert "deactivatedAt" not in doc

    def test_created_at_is_recent(self, coupons_collection, mongodb_container):
        before = _utcnow()
        coupon_id = create_coupon_flow(input_=_input(mongodb_container))
        doc = coupons_collection.find_one({"couponId": coupon_id})
        assert doc["createdAt"] >= before

    def test_repeated_runs_are_idempotent(self, coupons_collection, mongodb_container):
        # "already-processed record" — re-running upserts the same couponId,
        # collection still contains exactly one document for that week
        first = create_coupon_flow(input_=_input(mongodb_container))
        second = create_coupon_flow(input_=_input(mongodb_container))
        third = create_coupon_flow(input_=_input(mongodb_container))
        assert first == second == third
        assert coupons_collection.count_documents({"couponId": first}) == 1

    def test_upsert_preserves_original_created_at(
        self, coupons_collection, mongodb_container
    ):
        # createdAt is $setOnInsert — must not be bumped on a re-run
        coupon_id = create_coupon_flow(input_=_input(mongodb_container))
        first_created_at = coupons_collection.find_one({"couponId": coupon_id})["createdAt"]
        time.sleep(0.05)
        create_coupon_flow(input_=_input(mongodb_container))
        second_created_at = coupons_collection.find_one({"couponId": coupon_id})["createdAt"]
        assert first_created_at == second_created_at

    def test_upsert_clears_stale_deactivated_at(self, coupons_collection, mongodb_container):
        # If a previously-deactivated coupon exists for next week, re-running
        # the flow re-activates it and clears deactivatedAt
        expected = build_weekly_coupon()
        coupons_collection.insert_one({
            "couponId": expected["couponId"],
            "name": "Old name",
            "status": "DEACTIVATED",
            "validFrom": expected["validFrom"],
            "validTo": expected["validTo"],
            "createdAt": _utcnow() - timedelta(days=60),
            "deactivatedAt": _utcnow() - timedelta(days=5),
        })
        create_coupon_flow(input_=_input(mongodb_container))
        doc = coupons_collection.find_one({"couponId": expected["couponId"]})
        assert doc["status"] == "INACTIVE"
        assert "deactivatedAt" not in doc
