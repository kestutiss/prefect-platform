import pytest
from prefect.testing.utilities import prefect_test_harness
from pymongo import MongoClient
from testcontainers.mongodb import MongoDbContainer

from migrations.mongodb.runner import MigrationRunner


@pytest.fixture(autouse=True, scope="session")
def prefect_test_harness_fixture():
    """Spin up an in-process Prefect environment for the whole test session.

    Flows run synchronously without needing a running server or worker.
    """
    with prefect_test_harness():
        yield


# ---------------------------------------------------------------------------
# MongoDB fixtures — used only by tests that explicitly request them
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def mongodb_container():
    """Start a real MongoDB 7 container once per test session."""
    with MongoDbContainer("mongo:7") as container:
        yield container


@pytest.fixture(scope="session")
def mongo_db(mongodb_container):
    """Return the 'account' database with all migrations applied."""
    client = MongoClient(mongodb_container.get_connection_url())
    db = client["account"]
    MigrationRunner(db).run_pending()
    yield db
    client.close()


@pytest.fixture()
def coupons_collection(mongo_db):
    """Return the coupons collection, cleared before and after each test."""
    coll = mongo_db["coupons"]
    coll.delete_many({})
    yield coll
    coll.delete_many({})
