"""Testing the MongoDB core of cachier."""

# standard library imports
import datetime
from time import sleep

# third party imports
import pytest
from birch import Birch  # type: ignore[import-not-found]

# local imports
from cachier import cachier
# from cachier.cores.base import RecalculationNeeded
# from cachier.cores.odbc import _OdbcCore


class CfgKey:
    """Configuration keys for testing."""
    TEST_VS_DOCKERIZED_MYSQL = "TEST_VS_DOCKERIZED_MYSQL"
    TEST_PYODBC_CONNECTION_STRING = "TEST_PYODBC_CONNECTION_STRING"


CFG = Birch(
    namespace="cachier",
    defaults={CfgKey.TEST_VS_DOCKERIZED_MYSQL: False},
)

# Configuration for ODBC connection for tests
CONCT_STR = CFG.mget(CfgKey.TEST_PYODBC_CONNECTION_STRING)
# TABLE_NAME = "test_cache_table"


@pytest.mark.odbc
def test_odbc_entry_creation_and_retrieval(odbc_core):
    """Test inserting and retrieving an entry from ODBC cache."""

    @cachier(backend='odbc', odbc_connection_string=CONCT_STR)
    def sample_function(arg_1, arg_2):
        return arg_1 + arg_2

    sample_function.clear_cache()
    assert sample_function(1, 2) == 3  # Test cache miss and insertion
    assert sample_function(1, 2) == 3  # Test cache hit


@pytest.mark.odbc
def test_odbc_stale_after(odbc_core):
    """Test ODBC core handling stale_after parameter."""
    stale_after = datetime.timedelta(seconds=1)

    @cachier(backend='odbc', odbc_connection_string=CONCT_STR, stale_after=stale_after)
    def stale_test_function(arg_1, arg_2):
        return arg_1 + arg_2 + datetime.datetime.now().timestamp()  # Add timestamp to ensure unique values

    initial_value = stale_test_function(5, 10)
    sleep(2)  # Wait for the entry to become stale
    assert stale_test_function(5, 10) != initial_value  # Should recompute since stale


@pytest.mark.odbc
def test_odbc_clear_cache(odbc_core):
    """Test clearing the ODBC cache."""
    @cachier(backend='odbc', odbc_connection_string=CONCT_STR)
    def clearable_function(arg):
        return arg

    clearable_function.clear_cache()  # Ensure clean state
    assert clearable_function(3) == 3  # Populate cache
    clearable_function.clear_cache()  # Clear cache
    # The next call should recompute result indicating that cache was cleared
    assert clearable_function(3) == 3


@pytest.mark.odbc
def test_odbc_being_calculated_flag(odbc_core):
    """Test handling of 'being_calculated' flag in ODBC core."""
    @cachier(backend='odbc', odbc_connection_string=CONCT_STR)
    def slow_function(arg):
        sleep(2)  # Simulate long computation
        return arg * 2

    slow_function.clear_cache()
    result1 = slow_function(4)
    result2 = slow_function(4)  # Should hit cache, not wait for recalculation
    assert result1 == result2
