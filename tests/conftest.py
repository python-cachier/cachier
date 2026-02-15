"""Pytest configuration and shared fixtures for cachier tests."""

import pytest


@pytest.fixture(scope="session", autouse=True)
def cleanup_mongo_clients():
    """Clean up any MongoDB clients created during tests.

    This fixture runs automatically after all tests complete.

    """
    yield

    try:
        from tests.mongo_tests.helpers import _cleanup_mongo_client
    except ImportError:
        return

    _cleanup_mongo_client()
