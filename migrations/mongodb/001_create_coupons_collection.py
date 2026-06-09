from pymongo import ASCENDING

VERSION = "001"
NAME = "create_coupons_collection"


def up(db) -> None:
    """Create coupons collection indexes.

    MongoDB creates the collection implicitly on first insert;
    we only need to ensure indexes are in place before any reads.
    """
    coll = db["coupons"]
    coll.create_index("couponId", unique=True, name="idx_coupons_coupon_id")
    coll.create_index(
        [("status", ASCENDING), ("validTo", ASCENDING)],
        name="idx_coupons_status_valid_to",
    )
    coll.create_index("createdAt", name="idx_coupons_created_at")
