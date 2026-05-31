import pytest
from prefect.testing.utilities import prefect_test_harness


@pytest.fixture(autouse=True, scope="session")
def prefect_test_harness_fixture():
    """Spin up an in-process Prefect environment for the whole test session.

    Flows run synchronously without needing a running server or worker.
    """
    with prefect_test_harness():
        yield
