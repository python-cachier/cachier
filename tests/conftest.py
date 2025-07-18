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
        # If the module wasn't imported or client wasn't created,
        # then there's nothing to clean up
        pass


@pytest.fixture(autouse=True)
def isolated_cache_directory(tmp_path, monkeypatch, request, worker_id):
    """Ensure each test gets an isolated cache directory.

    This is especially important for pickle tests when running in parallel.
    Each pytest-xdist worker gets its own cache directory to avoid conflicts.

    """
    if "pickle" in request.node.keywords:
        # Create a unique cache directory for this test
        if worker_id == "master":
            # Not running in parallel mode
            cache_dir = tmp_path / "cachier_cache"
        else:
            # Running with pytest-xdist - use worker-specific directory
            cache_dir = tmp_path / f"cachier_cache_{worker_id}"

        cache_dir.mkdir(exist_ok=True, parents=True)

        # Monkeypatch the global cache directory for this test
        import cachier.config

        monkeypatch.setattr(
            cachier.config._global_params, "cache_dir", str(cache_dir)
        )

        # Also set environment variable as a backup
        monkeypatch.setenv("CACHIER_TEST_CACHE_DIR", str(cache_dir))


def pytest_addoption(parser):
    """Add custom command line options for parallel testing."""
    parser.addoption(
        "--parallel",
        action="store_true",
        default=False,
        help="Run tests in parallel using pytest-xdist",
    )
    parser.addoption(
        "--parallel-workers",
        action="store",
        default="auto",
        help="Number of parallel workers (default: auto)",
    )
