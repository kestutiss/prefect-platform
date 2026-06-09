"""Apply pending MongoDB migrations to the dev database.

Usage:
    make migrate               # uses MONGO_URI env var or localhost default
    MONGO_URI=mongodb://... make migrate
"""
import os

from pymongo import MongoClient

from migrations.mongodb.runner import MigrationRunner

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
DATABASE = "account"


def main() -> None:
    print(f"Connecting to {MONGO_URI!r}, database={DATABASE!r}")
    client = MongoClient(MONGO_URI)
    try:
        runner = MigrationRunner(client[DATABASE])
        runner.run_pending()
        print("Migrations complete.")
    finally:
        client.close()


if __name__ == "__main__":
    main()
