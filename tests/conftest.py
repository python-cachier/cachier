"""Pytest configuration and shared fixtures for cachier tests."""

from contextlib import suppress

import pytest


@pytest.fixture(scope="session", autouse=True)
def cleanup_mongo_clients():
    """Clean up any MongoDB clients created during tests.

    This fixture runs automatically after all tests complete.

    """
    # Let tests run
    yield

    # Cleanup after all tests
    try:
        from tests.test_mongo_core import _test_mongetter
    except ImportError:
        return

    client = getattr(_test_mongetter, "client", None)
    if client is None:
        return

    # pymongo_inmemory leaves an internal health-check client open.
    with suppress(Exception):
        client._mongod._client.close()  # type: ignore[attr-defined]
    client.close()

    # Remove the client attribute so future test runs start fresh
    delattr(_test_mongetter, "client")
