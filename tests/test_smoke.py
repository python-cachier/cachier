"""Smoke tests for cachier - fast, no external service dependencies."""

import datetime

import pytest

import cachier
from cachier import cachier as cachier_decorator
from cachier.config import get_global_params, set_global_params


@pytest.mark.smoke
def test_import():
    """Test that cachier can be imported and has a version."""
    assert cachier.__version__
    assert isinstance(cachier.__version__, str)


@pytest.mark.smoke
def test_get_global_params():
    """Test that global params can be retrieved."""
    params = get_global_params()
    assert params is not None
    assert hasattr(params, "backend")
    assert hasattr(params, "stale_after")


@pytest.mark.smoke
def test_invalid_backend():
    """Test that an invalid backend raises a ValueError."""
    with pytest.raises(ValueError, match="specified an invalid core"):

        @cachier_decorator(backend="invalid_backend")
        def dummy():
            pass


@pytest.mark.smoke
def test_pickle_backend_basic(tmp_path):
    """Test basic caching with the pickle backend."""
    call_count = 0

    @cachier_decorator(cache_dir=tmp_path, backend="pickle")
    def add(a, b):
        nonlocal call_count
        call_count += 1
        return a + b

    add.clear_cache()
    assert call_count == 0
    assert add(1, 2) == 3
    assert call_count == 1
    assert add(1, 2) == 3
    assert call_count == 1  # cache hit
    add.clear_cache()


@pytest.mark.smoke
def test_memory_backend_basic():
    """Test basic caching with the memory backend."""
    call_count = 0

    @cachier_decorator(backend="memory")
    def multiply(a, b):
        nonlocal call_count
        call_count += 1
        return a * b

    multiply.clear_cache()
    assert call_count == 0
    assert multiply(3, 4) == 12
    assert call_count == 1
    assert multiply(3, 4) == 12
    assert call_count == 1  # cache hit
    multiply.clear_cache()


@pytest.mark.smoke
def test_clear_cache(tmp_path):
    """Test that clear_cache resets the cache."""
    call_count = 0

    @cachier_decorator(cache_dir=tmp_path, backend="pickle")
    def func():
        nonlocal call_count
        call_count += 1
        return 42

    func.clear_cache()
    func()
    func()
    assert call_count == 1
    func.clear_cache()
    func()
    assert call_count == 2
    func.clear_cache()


@pytest.mark.smoke
def test_pickle_backend_stale_after(tmp_path):
    """Test that stale_after=timedelta(0) always recalculates."""
    call_count = 0

    @cachier_decorator(
        cache_dir=tmp_path,
        backend="pickle",
        stale_after=datetime.timedelta(seconds=0),
        next_time=False,
    )
    def func(x):
        nonlocal call_count
        call_count += 1
        return x * 2

    func.clear_cache()
    assert func(5) == 10
    assert func(5) == 10
    assert call_count == 2  # always recalculates when immediately stale
    func.clear_cache()


@pytest.mark.smoke
def test_allow_none(tmp_path):
    """Test that allow_none=True caches None return values."""
    call_count = 0

    @cachier_decorator(cache_dir=tmp_path, backend="pickle", allow_none=True)
    def returns_none():
        nonlocal call_count
        call_count += 1
        return None

    returns_none.clear_cache()
    assert returns_none() is None
    assert returns_none() is None
    assert call_count == 1  # second call uses cache
    returns_none.clear_cache()


@pytest.mark.smoke
def test_set_global_params_backend():
    """Test that set_global_params changes the active backend."""
    original = get_global_params().backend
    try:
        set_global_params(backend="memory")
        assert get_global_params().backend == "memory"
    finally:
        set_global_params(backend=original)


@pytest.mark.smoke
def test_cache_dpath_pickle(tmp_path):
    """Test that cache_dpath returns a path for the pickle backend."""

    @cachier_decorator(cache_dir=tmp_path, backend="pickle")
    def func():
        return 1

    assert func.cache_dpath() is not None


@pytest.mark.smoke
def test_cache_dpath_memory():
    """Test that cache_dpath returns None for the memory backend."""

    @cachier_decorator(backend="memory")
    def func():
        return 1

    assert func.cache_dpath() is None
