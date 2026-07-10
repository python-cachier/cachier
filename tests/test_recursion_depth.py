"""Tests for recursion depth protection in hash function."""

from datetime import timedelta

import pytest

from cachier import cachier


@pytest.mark.parametrize(
    "backend",
    [
        pytest.param("memory", marks=pytest.mark.memory),
        pytest.param("pickle", marks=pytest.mark.pickle),
    ],
)
def test_moderately_nested_structures_work(backend, tmp_path):
    """Verify that moderately nested structures (< 100 levels) work fine."""
    call_count = 0

    decorator_kwargs = {"backend": backend, "stale_after": timedelta(seconds=120)}
    if backend == "pickle":
        decorator_kwargs["cache_dir"] = tmp_path

    @cachier(**decorator_kwargs)
    def process_nested(data):
        nonlocal call_count
        call_count += 1
        return "processed"

    # Create a nested structure with 50 levels (well below the 100 limit)
    nested_list = []
    current = nested_list
    for _ in range(50):
        inner = []
        current.append(inner)
        current = inner
    current.append("leaf")

    # Should work without issues
    result1 = process_nested(nested_list)
    assert result1 == "processed"
    assert call_count == 1

    # Second call should hit cache
    result2 = process_nested(nested_list)
    assert result2 == "processed"
    assert call_count == 1

    process_nested.clear_cache()


@pytest.mark.parametrize(
    "backend",
    [
        pytest.param("memory", marks=pytest.mark.memory),
        pytest.param("pickle", marks=pytest.mark.pickle),
    ],
)
def test_deeply_nested_structures_raise_error(backend, tmp_path):
    """Verify that deeply nested structures (> 100 levels) raise RecursionError."""
    decorator_kwargs = {"backend": backend, "stale_after": timedelta(seconds=120)}
    if backend == "pickle":
        decorator_kwargs["cache_dir"] = tmp_path

    @cachier(**decorator_kwargs)
    def process_nested(data):
        return "processed"

    # Create a nested structure with 150 levels (exceeds the 100 limit)
    nested_list = []
    current = nested_list
    for _ in range(150):
        inner = []
        current.append(inner)
        current = inner
    current.append("leaf")

    # Should raise RecursionError with a clear message
    with pytest.raises(
        RecursionError,
        match=r"Maximum recursion depth \(100\) exceeded while hashing nested",
    ):
        process_nested(nested_list)


@pytest.mark.parametrize(
    "backend",
    [
        pytest.param("memory", marks=pytest.mark.memory),
        pytest.param("pickle", marks=pytest.mark.pickle),
    ],
)
def test_nested_dicts_respect_depth_limit(backend, tmp_path):
    """Verify that nested dictionaries also respect the depth limit."""
    decorator_kwargs = {"backend": backend, "stale_after": timedelta(seconds=120)}
    if backend == "pickle":
        decorator_kwargs["cache_dir"] = tmp_path

    @cachier(**decorator_kwargs)
    def process_dict(data):
        return "processed"

    # Create nested dictionaries beyond the limit
    nested_dict = {}
    current = nested_dict
    for i in range(150):
        current[f"level_{i}"] = {}
        current = current[f"level_{i}"]
    current["leaf"] = "value"

    # Should raise RecursionError
    with pytest.raises(
        RecursionError,
        match=r"Maximum recursion depth \(100\) exceeded while hashing nested",
    ):
        process_dict(nested_dict)


@pytest.mark.parametrize(
    "backend",
    [
        pytest.param("memory", marks=pytest.mark.memory),
        pytest.param("pickle", marks=pytest.mark.pickle),
    ],
)
def test_nested_tuples_respect_depth_limit(backend, tmp_path):
    """Verify that nested tuples also respect the depth limit."""
    decorator_kwargs = {"backend": backend, "stale_after": timedelta(seconds=120)}
    if backend == "pickle":
        decorator_kwargs["cache_dir"] = tmp_path

    @cachier(**decorator_kwargs)
    def process_tuple(data):
        return "processed"

    # Create nested tuples beyond the limit
    nested_tuple = ("leaf",)
    for _ in range(150):
        nested_tuple = (nested_tuple,)

    # Should raise RecursionError
    with pytest.raises(
        RecursionError,
        match=r"Maximum recursion depth \(100\) exceeded while hashing nested",
    ):
        process_tuple(nested_tuple)
