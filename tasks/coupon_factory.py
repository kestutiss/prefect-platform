"""Pure helpers to build the weekly coupon — no Prefect, no MongoDB, no I/O.

Naming is deterministic: the ISO week number indexes into WORDS, so the same
week always produces the same coupon name (and the flow stays idempotent
when re-run within the same week).
"""
from datetime import date, datetime, timedelta, timezone

# 26-word dictionary keyed by ISO week. Rotates ~twice per year.
WORDS: list[str] = [
    "Aurora", "Bliss", "Cascade", "Dazzle", "Eclipse", "Fable",
    "Galaxy", "Horizon", "Iris", "Jade", "Kismet", "Lumen",
    "Mirage", "Nebula", "Opal", "Prism", "Quartz", "Rune",
    "Solstice", "Tide", "Umbra", "Vortex", "Whisper", "Xenon",
    "Yonder", "Zenith",
]


def _next_iso_week(today: date | None = None) -> tuple[int, int]:
    """Return (iso_year, iso_week) for the upcoming Monday's week.

    If today IS Monday, returns next Monday's week (not the current week) —
    matches the colloquial meaning of "next week".
    """
    today = today or date.today()
    days_to_add = (7 - today.weekday()) % 7 or 7
    next_monday = today + timedelta(days=days_to_add)
    iso_year, iso_week, _ = next_monday.isocalendar()
    return iso_year, iso_week


def generate_coupon_name(iso_week: int) -> str:
    """Pick a deterministic name from WORDS based on the ISO week number."""
    return f"{WORDS[iso_week % len(WORDS)]} Week"


def build_weekly_coupon(today: date | None = None) -> dict:
    """Build the MongoDB document for next week's coupon.

    Validity period covers the whole next ISO week (Mon 00:00 → Sun 23:59:59).
    couponId format: WEEK-<iso_year>-W<iso_week:02d> — unique per ISO week.
    """
    iso_year, iso_week = _next_iso_week(today)
    monday = date.fromisocalendar(iso_year, iso_week, 1)
    sunday = date.fromisocalendar(iso_year, iso_week, 7)
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    return {
        "couponId": f"WEEK-{iso_year}-W{iso_week:02d}",
        "name": generate_coupon_name(iso_week),
        "status": "INACTIVE",
        "validFrom": datetime.combine(monday, datetime.min.time()),
        "validTo": datetime.combine(sunday, datetime.max.time()).replace(microsecond=0),
        "createdAt": now,
    }
