"""Pytest configuration and shared fixtures for cachier tests."""

import logging
import os
import re
from typing import Optional
from urllib.parse import parse_qs, unquote, urlencode, urlparse, urlunparse

import pytest

logger = logging.getLogger(__name__)


def _worker_schema_name(worker_id: str) -> Optional[str]:
    """Return a safe SQL schema name for an xdist worker ID."""
    match = re.fullmatch(r"gw(\d+)", worker_id)
    if match is None:
        return None
    return f"test_worker_{match.group(1)}"


def _build_worker_url(original_url: str, schema_name: str) -> str:
    """Return a copy of original_url with search_path set to schema_name."""
    parsed = urlparse(original_url)
    query_params = parse_qs(parsed.query)

    if "options" in query_params:
        current_options = unquote(query_params["options"][0])
        new_options = f"{current_options} -csearch_path={schema_name}"
    else:
        new_options = f"-csearch_path={schema_name}"

    query_params["options"] = [new_options]
    new_query = urlencode(query_params, doseq=True)
    return urlunparse(
        (
            parsed.scheme,
            parsed.netloc,
            parsed.path,
            parsed.params,
            new_query,
            parsed.fragment,
        )
    )


@pytest.fixture(scope="session")
def worker_sql_connection(request: pytest.FixtureRequest) -> Optional[str]:
    """Create the worker-specific PostgreSQL schema once per xdist worker session.

    Returns the worker-specific connection URL, or None when schema isolation is not
    needed (serial run or non-PostgreSQL backend). The schema is created with
    ``CREATE SCHEMA IF NOT EXISTS`` so this fixture is safe to run even if the schema
    already exists from a previous interrupted run.

    A non-None return value means "use this URL"; schema creation is attempted but may
    fail silently (e.g. if SQLAlchemy is not installed or the DB is unreachable). Tests
    that depend on the schema will fail at the DB level with a diagnostic error.

    """
    # Avoid touching SQL backends entirely when no SQL tests are collected.
    has_sql_tests = any("sql" in item.keywords for item in request.session.items)
    if not has_sql_tests:
        return None

    worker_id = os.environ.get("PYTEST_XDIST_WORKER", "master")
    if worker_id == "master":
        return None

    original_url = os.environ.get("SQLALCHEMY_DATABASE_URL", "sqlite:///:memory:")
    if "postgresql" not in original_url:
        return None

    schema_name = _worker_schema_name(worker_id)
    if schema_name is None:
        logger.warning("Unexpected worker ID for SQL schema isolation: %s", worker_id)
        return None

    new_url = _build_worker_url(original_url, schema_name)

    engine = None
    try:
        from sqlalchemy import create_engine, text

        engine = create_engine(original_url)
        with engine.connect() as conn:
            conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema_name}"))
            conn.commit()
    except Exception as e:
        logger.debug("Failed to create schema %s: %s", schema_name, e)
    finally:
        if engine is not None:
            engine.dispose()

    return new_url


@pytest.fixture(autouse=True)
def inject_worker_schema_for_sql_tests(monkeypatch, request, worker_sql_connection):
    """Automatically inject worker-specific schema into SQL connection string.

    This fixture enables parallel SQL test execution by giving each pytest-xdist worker
    its own PostgreSQL schema, preventing table creation conflicts.

    Schema creation is handled once per worker session by
    :func:`worker_sql_connection`. This fixture only performs lightweight
    per-test monkeypatching of the environment variable and module constant.

    """
    if "sql" not in request.node.keywords or worker_sql_connection is None:
        yield
        return

    monkeypatch.setenv("SQLALCHEMY_DATABASE_URL", worker_sql_connection)

    import tests.sql_tests.test_sql_core

    monkeypatch.setattr(tests.sql_tests.test_sql_core, "SQL_CONN_STR", worker_sql_connection)

    yield


@pytest.fixture
def worker_id(request):
    """Get the pytest-xdist worker ID."""
    return os.environ.get("PYTEST_XDIST_WORKER", "master")


@pytest.fixture(autouse=True)
def isolated_cache_directory(tmp_path, monkeypatch, request, worker_id):
    """Ensure each test gets an isolated cache directory.

    This is especially important for pickle and maxage tests when running in parallel. Each pytest-xdist worker gets its
    own cache directory to avoid conflicts.

    Only applies when running in parallel mode (pytest-xdist), to avoid breaking tests that use module-level path
    constants computed from the default cache directory at import time.

    """
    if worker_id != "master" and ("pickle" in request.node.keywords or "maxage" in request.node.keywords):
        cache_dir = tmp_path / f"cachier_cache_{worker_id}"

        cache_dir.mkdir(exist_ok=True, parents=True)

        # Monkeypatch the global cache directory for this test
        import cachier.config

        monkeypatch.setattr(cachier.config._global_params, "cache_dir", str(cache_dir))


def pytest_collection_modifyitems(items):
    """Mark local backends as serial-local for the split test runner flow."""
    for item in items:
        if "memory" in item.keywords or "pickle" in item.keywords:
            item.add_marker(pytest.mark.seriallocal)


@pytest.fixture(scope="session", autouse=True)
def cleanup_test_schemas(request):
    """Clean up test schemas after all tests complete.

    This fixture ensures that worker-specific PostgreSQL schemas created during parallel test execution are properly
    cleaned up.

    """
    yield  # Let all tests run first

    # Cleanup after all tests
    worker_id = os.environ.get("PYTEST_XDIST_WORKER", "master")

    if worker_id != "master":
        # Clean up the worker-specific schema
        original_url = os.environ.get("SQLALCHEMY_DATABASE_URL", "")

        if "postgresql" in original_url:
            schema_name = _worker_schema_name(worker_id)
            if schema_name is None:
                logger.warning("Unexpected worker ID for SQL schema cleanup: %s", worker_id)
                return

            try:
                from sqlalchemy import create_engine, text

                # Parse URL to remove any schema options for cleanup
                parsed = urlparse(original_url)
                query_params = parse_qs(parsed.query)

                # Remove options parameter if it exists
                query_params.pop("options", None)

                # Rebuild clean URL
                clean_query = urlencode(query_params, doseq=True) if query_params else ""
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
                    conn.execute(text(f"DROP SCHEMA IF EXISTS {schema_name} CASCADE"))
                    conn.commit()
                engine.dispose()
            except Exception as e:
                # If cleanup fails, it's not critical
                logger.debug("Failed to cleanup schema %s: %s", schema_name, e)
