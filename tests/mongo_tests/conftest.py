"""Shared Mongo test fixtures."""

import pytest

from .helpers import _cleanup_mongo_client


@pytest.fixture(scope="session", autouse=True)
def cleanup_mongo_client_fixture():
    """Release cached Mongo client resources after the test session."""
    yield
    _cleanup_mongo_client()
