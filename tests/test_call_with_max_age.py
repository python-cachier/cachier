import time
from datetime import timedelta

import pytest

import cachier


@pytest.mark.maxage
def test_call_with_max_age():
    @cachier.cachier()
    def test_func(a, b):
        return a + b

    # First call: should compute and cache
    val1 = test_func(1, 2)
    assert val1 == 3
    # Second call: should use cache
    val2 = test_func(1, 2)
    assert val2 == 3
    # Wait for cache to become stale
    time.sleep(1.0)
    # Should trigger recalculation (stale)
    val3 = test_func(1, 2, max_age=timedelta(seconds=0.5))
    assert val3 == 3


@pytest.mark.maxage
def test_max_age_stricter_than_stale_after():
    import time

    import cachier

    @cachier.cachier(stale_after=timedelta(seconds=2))
    def f(x):
        return time.time()

    f.clear_cache()
    v1 = f(1)
    v2 = f(1)
    assert v1 == v2  # cache hit
    time.sleep(1)
    v3 = f(1, max_age=timedelta(seconds=0.5))
    assert v3 != v1  # max_age stricter, triggers recalc


@pytest.mark.maxage
def test_max_age_looser_than_stale_after():
    import time

    import cachier

    @cachier.cachier(stale_after=timedelta(seconds=1))
    def f(x):
        return time.time()

    f.clear_cache()
    v1 = f(1)
    v2 = f(1)
    assert v1 == v2
    time.sleep(1.1)
    v3 = f(1, max_age=timedelta(seconds=5))
    assert v3 != v1  # max_age looser, but stale_after still applies (stricter)


@pytest.mark.maxage
def test_max_age_none_defaults_to_stale_after():
    import time

    import cachier

    @cachier.cachier(stale_after=timedelta(seconds=1))
    def f(x):
        return time.time()

    f.clear_cache()
    v1 = f(1)
    time.sleep(1.1)
    v2 = f(1, max_age=None)
    assert v2 != v1  # Should trigger recalc (stale_after applies)


@pytest.mark.maxage
def test_negative_max_age_triggers_recalc():
    import time

    import cachier

    @cachier.cachier(stale_after=timedelta(seconds=100))
    def f(x):
        return time.time()

    f.clear_cache()
    v1 = f(1)
    v2 = f(1, max_age=timedelta(seconds=-1), cachier__verbose=True)
    assert v2 != v1  # Negative max_age always triggers recalc


@pytest.mark.maxage
def test_max_age_zero():
    import time

    import cachier

    @cachier.cachier(stale_after=timedelta(seconds=100))
    def f(x):
        return time.time()

    f.clear_cache()
    v1 = f(1)
    # Add a small sleep to ensure measurable time difference on all platforms
    time.sleep(1)
    v2 = f(1, max_age=timedelta(seconds=0))
    assert v2 != v1  # Zero max_age always triggers recalc


@pytest.mark.maxage
def test_max_age_with_next_time():
    import time

    import cachier

    @cachier.cachier(stale_after=timedelta(seconds=1), next_time=True)
    def f(x):
        return time.time()

    f.clear_cache()
    v1 = f(1)
    time.sleep(1.1)
    v2 = f(1, max_age=timedelta(seconds=0.5))
    # With next_time=True, should return stale value (v1) while
    # triggering a recalculation in the background
    assert v2 == v1
