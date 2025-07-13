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
        from tests.test_mongo_core import _test_mongetter
        if hasattr(_test_mongetter, "client"):
            # Close the MongoDB client to avoid ResourceWarning
            _test_mongetter.client.close()
            # Remove the client attribute so future test runs start fresh
            delattr(_test_mongetter, "client")
    except (ImportError, AttributeError):
        # If the module wasn't imported or client wasn't created, nothing to clean up
        pass