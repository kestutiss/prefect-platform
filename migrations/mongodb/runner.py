from datetime import datetime, timezone
from importlib import import_module
from pathlib import Path


class MigrationRunner:
    """Applies numbered MongoDB migration scripts in version order.

    Tracks applied migrations in the ``_migrations`` collection so each
    script runs exactly once, regardless of how many times run_pending() is called.
    """

    _TRACKING_COLLECTION = "_migrations"

    def __init__(self, db) -> None:
        self._db = db
        self._tracking = db[self._TRACKING_COLLECTION]

    def _applied_versions(self) -> set[str]:
        return {doc["version"] for doc in self._tracking.find({}, {"version": 1})}

    def run_pending(self) -> None:
        """Import and run every migration not yet recorded in _migrations."""
        applied = self._applied_versions()
        migrations_dir = Path(__file__).parent
        for path in sorted(migrations_dir.glob("[0-9]*.py")):
            module = import_module(f"migrations.mongodb.{path.stem}")
            if module.VERSION in applied:
                continue
            module.up(self._db)
            self._tracking.insert_one(
                {
                    "version": module.VERSION,
                    "name": module.NAME,
                    "appliedAt": datetime.now(timezone.utc).replace(tzinfo=None),
                }
            )
