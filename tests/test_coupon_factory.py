"""Unit tests for tasks/coupon_factory.py — no MongoDB, no Prefect."""
from datetime import date

from tasks.coupon_factory import (
    WORDS,
    _next_iso_week,
    build_weekly_coupon,
    generate_coupon_name,
)


class TestGenerateCouponName:

    def test_returns_word_from_dictionary(self):
        name = generate_coupon_name(1)
        assert name.split()[0] in WORDS

    def test_is_deterministic_for_same_week(self):
        assert generate_coupon_name(15) == generate_coupon_name(15)

    def test_rotates_across_weeks(self):
        names = {generate_coupon_name(w) for w in range(len(WORDS))}
        assert len(names) == len(WORDS)  # every word appears exactly once over a full cycle

    def test_wraps_modulo_dictionary_length(self):
        # week N and week N+len(WORDS) produce the same name
        assert generate_coupon_name(3) == generate_coupon_name(3 + len(WORDS))


class TestNextIsoWeek:

    def test_monday_returns_next_week(self):
        # 2026-06-08 is a Monday (ISO week 24); next Monday is in week 25
        iso_year, iso_week = _next_iso_week(date(2026, 6, 8))
        assert (iso_year, iso_week) == (2026, 25)

    def test_sunday_returns_tomorrows_week(self):
        # 2026-06-07 is a Sunday; next Monday is the day after (week 24)
        iso_year, iso_week = _next_iso_week(date(2026, 6, 7))
        assert (iso_year, iso_week) == (2026, 24)

    def test_wednesday_returns_following_monday_week(self):
        # 2026-06-10 is a Wednesday (ISO week 24); next Monday is in week 25
        iso_year, iso_week = _next_iso_week(date(2026, 6, 10))
        assert (iso_year, iso_week) == (2026, 25)


class TestBuildWeeklyCoupon:

    def test_returns_required_fields(self):
        doc = build_weekly_coupon(today=date(2026, 6, 8))
        for field in ("couponId", "name", "status", "validFrom", "validTo", "createdAt"):
            assert field in doc

    def test_status_is_inactive(self):
        assert build_weekly_coupon(today=date(2026, 6, 8))["status"] == "INACTIVE"

    def test_coupon_id_format(self):
        doc = build_weekly_coupon(today=date(2026, 6, 8))
        assert doc["couponId"] == "WEEK-2026-W25"

    def test_name_matches_factory_for_that_week(self):
        doc = build_weekly_coupon(today=date(2026, 6, 8))
        assert doc["name"] == generate_coupon_name(25)

    def test_validity_spans_full_iso_week(self):
        doc = build_weekly_coupon(today=date(2026, 6, 8))
        # 2026 W25 = Mon 2026-06-15 → Sun 2026-06-21
        assert doc["validFrom"].date() == date(2026, 6, 15)
        assert doc["validTo"].date() == date(2026, 6, 21)
        assert doc["validFrom"].hour == 0 and doc["validFrom"].minute == 0
        assert doc["validTo"].hour == 23 and doc["validTo"].minute == 59

    def test_different_weeks_produce_different_ids(self):
        a = build_weekly_coupon(today=date(2026, 6, 8))    # week 25
        b = build_weekly_coupon(today=date(2026, 6, 15))   # week 26
        assert a["couponId"] != b["couponId"]
