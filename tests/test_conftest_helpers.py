"""Unit tests for private helpers in tests/conftest.py."""

import pytest

from tests.conftest import _build_worker_url, _worker_schema_name


class TestWorkerSchemaName:
    def test_valid_gw0(self):
        assert _worker_schema_name("gw0") == "test_worker_0"

    def test_valid_gw99(self):
        assert _worker_schema_name("gw99") == "test_worker_99"

    def test_master_returns_none(self):
        assert _worker_schema_name("master") is None

    def test_non_gw_id_returns_none(self):
        assert _worker_schema_name("worker1") is None

    def test_partial_match_returns_none(self):
        # "gw0extra" should not match the fullmatch pattern
        assert _worker_schema_name("gw0extra") is None


class TestBuildWorkerUrl:
    def test_url_without_options(self):
        url = "postgresql://user:pass@localhost/testdb"
        result = _build_worker_url(url, "test_worker_0")
        assert "options" in result
        assert "search_path%3Dtest_worker_0" in result or "search_path=test_worker_0" in result

    def test_url_with_existing_options_appends(self):
        url = "postgresql://user:pass@localhost/testdb?options=-cstatement_timeout%3D5000"
        result = _build_worker_url(url, "test_worker_1")
        # The new search_path must be present
        assert "search_path" in result
        assert "test_worker_1" in result
        # The original option must still be present
        assert "statement_timeout" in result

    def test_url_encoded_roundtrip(self):
        """Values with spaces survive encode/decode without corruption."""
        url = "postgresql://user:pass@localhost/testdb?options=-cwork_mem%3D64MB"
        result = _build_worker_url(url, "test_worker_2")
        # Scheme, host, and path must be preserved
        assert result.startswith("postgresql://user:pass@localhost/testdb")
        assert "test_worker_2" in result
        assert "work_mem" in result
