"""Test for caching enable/disable regression issue.

This test ensures that decorators defined when caching is disabled can still be enabled later via enable_caching().

"""

import datetime

import cachier


def test_enable_caching_after_decorator_definition():
    """Test that enable_caching() affects decorators when caching disabled."""
    # Start with caching disabled
    cachier.set_global_params(caching_enabled=False)

    call_count = 0

    # Use memory backend to avoid file cache persistence issues
    @cachier.cachier(backend="memory")
    def test_func(param):
        nonlocal call_count
        call_count += 1
        return f"result_{param}"

    # Initially caching is disabled, so function should be called each time
    result1 = test_func("a")
    assert result1 == "result_a"
    assert call_count == 1

    result2 = test_func("a")  # Same args, but caching disabled
    assert result2 == "result_a"
    assert call_count == 2  # Function called again

    # Now enable caching
    cachier.enable_caching()

    # Function should now be cached
    result3 = test_func("a")  # Same args, caching now enabled
    assert result3 == "result_a"
    assert call_count == 3  # Called once more to populate cache

    result4 = test_func("a")  # Same args, should come from cache
    assert result4 == "result_a"
    assert call_count == 3  # Not called again - came from cache!

    # Different args should still call the function
    result5 = test_func("b")
    assert result5 == "result_b"
    assert call_count == 4

    # Same new args should be cached
    result6 = test_func("b")
    assert result6 == "result_b"
    assert call_count == 4  # Not called again - came from cache!


def test_disable_caching_after_decorator_definition():
    """Test that disable_caching() affects decorators when caching enabled."""
    # Start with caching enabled
    cachier.enable_caching()

    call_count = 0

    # Use memory backend to avoid file cache persistence issues
    @cachier.cachier(backend="memory")
    def test_func(param):
        nonlocal call_count
        call_count += 1
        return f"result_{param}"

    # Caching is enabled, so function should be cached
    result1 = test_func("a")
    assert result1 == "result_a"
    assert call_count == 1

    result2 = test_func("a")  # Same args, should come from cache
    assert result2 == "result_a"
    assert call_count == 1  # Not called again

    # Now disable caching
    cachier.disable_caching()

    # Function should no longer use cache
    result3 = test_func("a")  # Same args, but caching now disabled
    assert result3 == "result_a"
    assert call_count == 2  # Function called again

    result4 = test_func("a")  # Same args, caching still disabled
    assert result4 == "result_a"
    assert call_count == 3  # Called again


def test_original_issue_scenario():
    """Test the exact scenario from the GitHub issue."""
    # Set up the same initial state as the issue
    cachier.set_global_params(caching_enabled=False, separate_files=True)

    class Test:
        def __init__(self, cache_ttl=None):
            self.counter = 0
            if cache_ttl is not None:
                stale_after = datetime.timedelta(seconds=cache_ttl)
                cachier.set_global_params(stale_after=stale_after)
                cachier.enable_caching()

        # Use memory backend to avoid file cache persistence issues
        @cachier.cachier(backend="memory")
        def test(self, param):
            self.counter += 1
            return param

    # This should work without assertion error
    t = Test(cache_ttl=1)
    result1 = t.test("a")
    assert result1 == "a"
    assert t.counter == 1

    result2 = t.test("a")  # Should come from cache
    assert result2 == "a"
    assert t.counter == 1  # Counter should not increment


if __name__ == "__main__":
    test_enable_caching_after_decorator_definition()
    test_disable_caching_after_decorator_definition()
    test_original_issue_scenario()
    print("All tests passed!")
