"""Pytest configuration and shared fixtures for cachier tests."""

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
        from tests.mongo_tests.conftest import _cleanup_mongo_client
    except ImportError:
        return

    _cleanup_mongo_client()
