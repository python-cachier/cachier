"""Test for variadic arguments (*args) handling in cachier."""

from datetime import timedelta

import pytest

from cachier import cachier


@pytest.mark.pickle
@pytest.mark.parametrize("backend", ["pickle", "memory"])
def test_varargs_different_cache_keys(backend):
    """Test *args get unique cache keys for different arguments."""
    call_count = 0

    @cachier(backend=backend, stale_after=timedelta(seconds=500))
    def get_data(*args):
        """Test function that accepts variadic arguments."""
        nonlocal call_count
        call_count += 1
        return f"Result for args: {args}, call #{call_count}"

    # Clear any existing cache
    get_data.clear_cache()
    call_count = 0

    # Test 1: Call with different arguments should produce different
    # cache entries
    result1 = get_data("print", "domains")
    assert call_count == 1
    assert "('print', 'domains')" in result1

    result2 = get_data("print", "users", "allfields")
    assert call_count == 2, (
        "Function should be called again with different args"
    )
    assert "('print', 'users', 'allfields')" in result2
    assert result1 != result2, (
        "Different args should produce different results"
    )

    # Test 2: Calling with the same arguments should use cache
    previous_call_count = call_count
    result3 = get_data("print", "domains")
    assert call_count == previous_call_count, (
        "Function should not be called again (cache hit)"
    )
    assert result3 == result1

    result4 = get_data("print", "users", "allfields")
    assert call_count == previous_call_count, (
        "Function should not be called again (cache hit)"
    )
    assert result4 == result2

    get_data.clear_cache()


@pytest.mark.pickle
def test_varargs_empty():
    """Test that functions with *args work with no arguments."""
    call_count = 0

    @cachier(stale_after=timedelta(seconds=500))
    def get_data(*args):
        """Test function that accepts variadic arguments."""
        nonlocal call_count
        call_count += 1
        return f"Result for args: {args}, call #{call_count}"

    get_data.clear_cache()
    call_count = 0

    # Call with no arguments
    result1 = get_data()
    assert call_count == 1
    assert "()" in result1

    # Second call should use cache
    previous_call_count = call_count
    result2 = get_data()
    assert call_count == previous_call_count
    assert result2 == result1

    get_data.clear_cache()


@pytest.mark.pickle
def test_varargs_with_regular_args():
    """Test regular and variadic arguments work correctly."""
    call_count = 0

    @cachier(stale_after=timedelta(seconds=500))
    def get_data(command, *args):
        """Test function with regular and variadic arguments."""
        nonlocal call_count
        call_count += 1
        return f"Command: {command}, args: {args}, call #{call_count}"

    get_data.clear_cache()
    call_count = 0

    # Test different calls
    result1 = get_data("print", "domains")
    assert call_count == 1

    result2 = get_data("print", "users", "allfields")
    assert call_count == 2
    assert result1 != result2

    result3 = get_data("list")
    assert call_count == 3
    assert result3 != result1
    assert result3 != result2

    # Test cache hits
    previous_call_count = call_count
    result4 = get_data("print", "domains")
    assert call_count == previous_call_count
    assert result4 == result1

    get_data.clear_cache()


@pytest.mark.pickle
def test_varkwargs_different_cache_keys():
    """Test **kwargs get unique cache keys for different arguments."""
    call_count = 0

    @cachier(stale_after=timedelta(seconds=500))
    def get_data(**kwargs):
        """Test function that accepts keyword variadic arguments."""
        nonlocal call_count
        call_count += 1
        return f"Result for kwargs: {kwargs}, call #{call_count}"

    get_data.clear_cache()
    call_count = 0

    # Test with different kwargs
    result1 = get_data(type="domains", action="print")
    assert call_count == 1

    result2 = get_data(type="users", action="print", fields="allfields")
    assert call_count == 2
    assert result1 != result2

    # Test cache hits
    previous_call_count = call_count
    result3 = get_data(type="domains", action="print")
    assert call_count == previous_call_count
    assert result3 == result1

    get_data.clear_cache()


@pytest.mark.pickle
def test_varargs_and_varkwargs():
    """Test that functions with both *args and **kwargs work correctly."""
    call_count = 0

    @cachier(stale_after=timedelta(seconds=500))
    def get_data(*args, **kwargs):
        """Test function with both variadic arguments."""
        nonlocal call_count
        call_count += 1
        return f"args: {args}, kwargs: {kwargs}, call #{call_count}"

    get_data.clear_cache()
    call_count = 0

    # Test different combinations
    result1 = get_data("print", "domains")
    assert call_count == 1

    result2 = get_data("print", "users", action="list")
    assert call_count == 2
    assert result1 != result2

    result3 = get_data(action="list", resource="domains")
    assert call_count == 3
    assert result3 != result1
    assert result3 != result2

    # Test cache hits
    result4 = get_data("print", "domains")
    assert call_count == 3
    assert result4 == result1

    get_data.clear_cache()


@pytest.mark.memory
def test_varargs_memory_backend():
    """Test that variadic arguments work with memory backend."""
    call_count = 0

    @cachier(backend="memory", stale_after=timedelta(seconds=500))
    def get_data(*args):
        """Test function that accepts variadic arguments."""
        nonlocal call_count
        call_count += 1
        return f"Result: {args}, call #{call_count}"

    get_data.clear_cache()
    call_count = 0

    result1 = get_data("a", "b")
    assert call_count == 1

    result2 = get_data("a", "b", "c")
    assert call_count == 2
    assert result1 != result2

    result3 = get_data("a", "b")
    assert call_count == 2
    assert result3 == result1

    get_data.clear_cache()


@pytest.mark.pickle
def test_keyword_only_parameters():
    """Test that functions with keyword-only parameters work correctly."""
    call_count = 0

    @cachier(stale_after=timedelta(seconds=500))
    def get_data(*args, kw_only):
        """Test function with keyword-only parameter."""
        nonlocal call_count
        call_count += 1
        return f"args={args}, kw_only={kw_only}, call #{call_count}"

    get_data.clear_cache()
    call_count = 0

    # Test with different keyword-only values
    result1 = get_data("a", "b", kw_only="value1")
    assert call_count == 1

    result2 = get_data("a", "b", kw_only="value2")
    assert call_count == 2
    assert result1 != result2

    # Test cache hit
    result3 = get_data("a", "b", kw_only="value1")
    assert call_count == 2
    assert result3 == result1

    get_data.clear_cache()


@pytest.mark.pickle
def test_keyword_only_with_default():
    """Test keyword-only parameters with defaults work correctly."""
    call_count = 0

    @cachier(stale_after=timedelta(seconds=500))
    def get_data(*args, kw_only="default"):
        """Test function with keyword-only parameter with default."""
        nonlocal call_count
        call_count += 1
        return f"args={args}, kw_only={kw_only}, call #{call_count}"

    get_data.clear_cache()
    call_count = 0

    # Test with default value
    result1 = get_data("a", "b")
    assert call_count == 1
    assert "kw_only=default" in result1

    # Test with explicit value
    result2 = get_data("a", "b", kw_only="explicit")
    assert call_count == 2
    assert result1 != result2

    # Test cache hit with default
    result3 = get_data("a", "b")
    assert call_count == 2
    assert result3 == result1

    # Test cache hit with explicit value
    result4 = get_data("a", "b", kw_only="explicit")
    assert call_count == 2
    assert result4 == result2

    get_data.clear_cache()


@pytest.mark.pickle
def test_mixed_varargs_keyword_only():
    """Test *args and keyword-only parameters work correctly."""
    call_count = 0

    @cachier(stale_after=timedelta(seconds=500))
    def get_data(regular, *args, kw_only, kw_with_default="default"):
        """Test function with mixed parameter types."""
        nonlocal call_count
        call_count += 1
        return (
            f"regular={regular}, args={args}, kw_only={kw_only}, "
            f"kw_with_default={kw_with_default}, call #{call_count}"
        )

    get_data.clear_cache()
    call_count = 0

    # Test different combinations
    result1 = get_data("r1", "a", "b", kw_only="k1")
    assert call_count == 1

    result2 = get_data("r1", "a", "b", kw_only="k2")
    assert call_count == 2
    assert result1 != result2

    result3 = get_data("r1", "a", "b", kw_only="k1", kw_with_default="custom")
    assert call_count == 3
    assert result3 != result1

    # Test cache hits
    result4 = get_data("r1", "a", "b", kw_only="k1")
    assert call_count == 3
    assert result4 == result1

    get_data.clear_cache()
