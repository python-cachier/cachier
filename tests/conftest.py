"""Pytest configuration and shared fixtures for cachier tests."""

import logging
import os
from urllib.parse import parse_qs, unquote, urlencode, urlparse, urlunparse

import pytest

logger = logging.getLogger(__name__)


@pytest.fixture(autouse=True)
def inject_worker_schema_for_sql_tests(monkeypatch, request):
    """Automatically inject worker-specific schema into SQL connection string.

    This fixture enables parallel SQL test execution by giving each pytest-
    xdist worker its own PostgreSQL schema, preventing table creation
    conflicts.

    """
    # Only apply to SQL tests
    if "sql" not in request.node.keywords:
        yield
        return

    worker_id = os.environ.get("PYTEST_XDIST_WORKER", "master")

    if worker_id == "master":
        # Not running in parallel, no schema isolation needed
        yield
        return

    # Get the original SQL connection string
    original_url = os.environ.get(
        "SQLALCHEMY_DATABASE_URL", "sqlite:///:memory:"
    )

    if "postgresql" in original_url:
        # Create worker-specific schema name
        schema_name = f"test_worker_{worker_id.replace('gw', '')}"

        # Parse the URL
        parsed = urlparse(original_url)

        # Get existing query parameters
        query_params = parse_qs(parsed.query)

        # Add or update the options parameter to set search_path
        if "options" in query_params:
            # Append to existing options
            current_options = unquote(query_params["options"][0])
            new_options = f"{current_options} -csearch_path={schema_name}"
        else:
            # Create new options
            new_options = f"-csearch_path={schema_name}"

        query_params["options"] = [new_options]

        # Rebuild the URL with updated query parameters
        new_query = urlencode(query_params, doseq=True)
        new_url = urlunparse(
            (
                parsed.scheme,
                parsed.netloc,
                parsed.path,
                parsed.params,
                new_query,
                parsed.fragment,
            )
        )

        # Override both the environment variable and the module constant
        monkeypatch.setenv("SQLALCHEMY_DATABASE_URL", new_url)

        # Also patch the SQL_CONN_STR constant used in tests
        import tests.test_sql_core

        monkeypatch.setattr(tests.test_sql_core, "SQL_CONN_STR", new_url)

        # Ensure schema creation by creating it before tests run
        try:
            from sqlalchemy import create_engine, text

            # Use original URL to create schema (without search_path)
            engine = create_engine(original_url)
            with engine.connect() as conn:
                conn.execute(
                    text(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
                )
                conn.commit()
            engine.dispose()
        except Exception as e:
            # If we can't create the schema, the test will fail anyway
            logger.debug(f"Failed to create schema {schema_name}: {e}")

    yield


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


@pytest.fixture(scope="session", autouse=True)
def cleanup_test_schemas(request):
    """Clean up test schemas after all tests complete.

    This fixture ensures that worker-specific PostgreSQL schemas created during
    parallel test execution are properly cleaned up.

    """
    yield  # Let all tests run first

    # Cleanup after all tests
    worker_id = os.environ.get("PYTEST_XDIST_WORKER", "master")

    if worker_id != "master":
        # Clean up the worker-specific schema
        original_url = os.environ.get("SQLALCHEMY_DATABASE_URL", "")

        if "postgresql" in original_url:
            schema_name = f"test_worker_{worker_id.replace('gw', '')}"

            try:
                from sqlalchemy import create_engine, text

                # Parse URL to remove any schema options for cleanup
                parsed = urlparse(original_url)
                query_params = parse_qs(parsed.query)

                # Remove options parameter if it exists
                query_params.pop("options", None)

                # Rebuild clean URL
                clean_query = (
                    urlencode(query_params, doseq=True) if query_params else ""
                )
                clean_url = urlunparse(
                    (
                        parsed.scheme,
                        parsed.netloc,
                        parsed.path,
                        parsed.params,
                        clean_query,
                        parsed.fragment,
                    )
                )

                engine = create_engine(clean_url)
                with engine.connect() as conn:
                    # Drop the schema and all its contents
                    conn.execute(
                        text(f"DROP SCHEMA IF EXISTS {schema_name} CASCADE")
                    )
                    conn.commit()
                engine.dispose()
            except Exception as e:
                # If cleanup fails, it's not critical
                logger.debug(f"Failed to cleanup schema {schema_name}: {e}")


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
