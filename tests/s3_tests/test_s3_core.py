"""Tests for the S3 caching core."""

import contextlib
import threading
import warnings
from datetime import timedelta
from random import random

import pytest

from tests.s3_tests.helpers import S3_DEPS_AVAILABLE, TEST_BUCKET, TEST_REGION, skip_if_missing

if S3_DEPS_AVAILABLE:
    import boto3
    from moto import mock_aws

    from cachier import cachier
    from cachier.cores.s3 import MissingS3Bucket, _S3Core


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_core(s3_bucket=TEST_BUCKET, s3_client=None, s3_prefix="cachier"):
    """Return a bare _S3Core (set_func must still be called before use)."""
    skip_if_missing()
    return _S3Core(
        hash_func=None,
        s3_bucket=s3_bucket,
        s3_prefix=s3_prefix,
        s3_client=s3_client,
    )


# ---------------------------------------------------------------------------
# Basic construction and validation
# ---------------------------------------------------------------------------


@pytest.mark.s3
def test_missing_bucket_raises():
    skip_if_missing()
    with pytest.raises(MissingS3Bucket):
        _S3Core(hash_func=None, s3_bucket=None)


@pytest.mark.s3
def test_missing_bucket_empty_string_raises():
    skip_if_missing()
    with pytest.raises(MissingS3Bucket):
        _S3Core(hash_func=None, s3_bucket="")


@pytest.mark.s3
def test_missing_boto3_warns(monkeypatch):
    skip_if_missing()
    import cachier.cores.s3 as s3_mod

    monkeypatch.setattr(s3_mod, "BOTO3_AVAILABLE", False)
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        with contextlib.suppress(Exception):
            _S3Core(hash_func=None, s3_bucket="bucket")
    assert any("boto3" in str(w.message) for w in caught)


# ---------------------------------------------------------------------------
# Core caching behaviour
# ---------------------------------------------------------------------------


@pytest.mark.s3
def test_s3_core_basic_caching(s3_bucket):
    """Cached result is returned on the second call."""

    @cachier(backend="s3", s3_bucket=s3_bucket, s3_region=TEST_REGION)
    def _cached(x):
        return random() + x

    _cached.clear_cache()
    val1 = _cached(1)
    val2 = _cached(1)
    assert val1 == val2


@pytest.mark.s3
def test_s3_core_different_args(s3_bucket):
    """Different arguments produce different cache entries."""

    @cachier(backend="s3", s3_bucket=s3_bucket, s3_region=TEST_REGION)
    def _cached(x):
        return random() + x

    _cached.clear_cache()
    val1 = _cached(1)
    val2 = _cached(2)
    assert val1 != val2


@pytest.mark.s3
def test_s3_core_skip_cache(s3_bucket):
    """cachier__skip_cache bypasses the cache."""

    @cachier(backend="s3", s3_bucket=s3_bucket, s3_region=TEST_REGION)
    def _cached(x):
        return random()

    _cached.clear_cache()
    val1 = _cached(1)
    val2 = _cached(1, cachier__skip_cache=True)
    assert val1 != val2


@pytest.mark.s3
def test_s3_core_overwrite_cache(s3_bucket):
    """cachier__overwrite_cache forces recalculation."""

    @cachier(backend="s3", s3_bucket=s3_bucket, s3_region=TEST_REGION)
    def _cached(x):
        return random()

    _cached.clear_cache()
    val1 = _cached(1)
    val2 = _cached(1, cachier__overwrite_cache=True)
    val3 = _cached(1)
    assert val1 != val2
    assert val2 == val3


@pytest.mark.s3
def test_s3_core_stale_after(s3_bucket):
    """A result older than stale_after is recomputed."""

    @cachier(backend="s3", s3_bucket=s3_bucket, s3_region=TEST_REGION, stale_after=timedelta(seconds=1))
    def _cached(x):
        return random()

    _cached.clear_cache()
    val1 = _cached(1)
    import time

    time.sleep(2)
    val2 = _cached(1)
    assert val1 != val2


@pytest.mark.s3
def test_s3_core_next_time(s3_bucket):
    """With next_time=True, stale result is returned immediately."""

    @cachier(
        backend="s3",
        s3_bucket=s3_bucket,
        s3_region=TEST_REGION,
        stale_after=timedelta(seconds=1),
        next_time=True,
    )
    def _cached(x):
        return random()

    _cached.clear_cache()
    val1 = _cached(1)
    import time

    time.sleep(2)
    val2 = _cached(1)  # stale; should return old value immediately
    assert val1 == val2


@pytest.mark.s3
def test_s3_core_allow_none(s3_bucket):
    """None results are cached when allow_none=True."""
    call_count = [0]

    @cachier(backend="s3", s3_bucket=s3_bucket, s3_region=TEST_REGION, allow_none=True)
    def _cached(x):
        call_count[0] += 1
        return None

    _cached.clear_cache()
    res1 = _cached(1)
    res2 = _cached(1)
    assert res1 is None
    assert res2 is None
    assert call_count[0] == 1


@pytest.mark.s3
def test_s3_core_none_not_cached_without_allow_none(s3_bucket):
    """None results are NOT cached when allow_none=False (default)."""
    call_count = [0]

    @cachier(backend="s3", s3_bucket=s3_bucket, s3_region=TEST_REGION, allow_none=False)
    def _cached(x):
        call_count[0] += 1
        return None

    _cached.clear_cache()
    _cached(1)
    _cached(1)
    assert call_count[0] == 2


@pytest.mark.s3
def test_s3_core_clear_cache(s3_bucket):
    """clear_cache removes all entries so the next call recomputes."""
    call_count = [0]

    @cachier(backend="s3", s3_bucket=s3_bucket, s3_region=TEST_REGION)
    def _cached(x):
        call_count[0] += 1
        return x * 2

    _cached.clear_cache()
    _cached(5)
    _cached(5)
    assert call_count[0] == 1
    _cached.clear_cache()
    _cached(5)
    assert call_count[0] == 2


@pytest.mark.s3
def test_s3_core_clear_being_calculated(s3_bucket):
    """clear_being_calculated resets the processing flag on all entries."""

    @cachier(backend="s3", s3_bucket=s3_bucket, s3_region=TEST_REGION)
    def _cached(x):
        return x

    _cached.clear_cache()
    _cached(1)
    _cached.clear_being_calculated()  # should not raise


# ---------------------------------------------------------------------------
# entry_size_limit
# ---------------------------------------------------------------------------


@pytest.mark.s3
def test_s3_entry_size_limit(s3_bucket):
    """Results larger than entry_size_limit are not cached."""
    call_count = [0]

    @cachier(backend="s3", s3_bucket=s3_bucket, s3_region=TEST_REGION, entry_size_limit=1)
    def _cached(x):
        call_count[0] += 1
        return list(range(1000))

    _cached.clear_cache()
    _cached(1)
    _cached(1)
    assert call_count[0] == 2


# ---------------------------------------------------------------------------
# delete_stale_entries
# ---------------------------------------------------------------------------


@pytest.mark.s3
def test_s3_delete_stale_entries(s3_bucket):
    """delete_stale_entries removes entries that are older than stale_after."""
    call_count = [0]

    @cachier(backend="s3", s3_bucket=s3_bucket, s3_region=TEST_REGION)
    def _cached(x):
        call_count[0] += 1
        return x

    _cached.clear_cache()
    _cached(1)
    assert call_count[0] == 1

    import time

    time.sleep(1)
    # Manually trigger stale entry cleanup for entries older than 0 seconds
    from cachier.cores.s3 import _S3Core

    # Access core via closure - we delete with a tiny stale_after so the entry qualifies
    client = boto3.client("s3", region_name=TEST_REGION)
    s3_core = _S3Core(
        hash_func=None,
        s3_bucket=s3_bucket,
        s3_client=client,
    )
    s3_core.set_func(_cached.__wrapped__ if hasattr(_cached, "__wrapped__") else lambda x: x)
    s3_core.delete_stale_entries(timedelta(seconds=0))

    # After deleting we expect a recompute
    _cached(1)
    assert call_count[0] == 2


# ---------------------------------------------------------------------------
# s3_client_factory
# ---------------------------------------------------------------------------


@pytest.mark.s3
def test_s3_client_factory(s3_bucket):
    """s3_client_factory is called each time to obtain the S3 client."""
    factory_calls = [0]

    def my_factory():
        factory_calls[0] += 1
        return boto3.client("s3", region_name=TEST_REGION)

    @cachier(backend="s3", s3_bucket=s3_bucket, s3_client_factory=my_factory)
    def _cached(x):
        return x * 3

    _cached.clear_cache()
    _cached(7)
    _cached(7)
    assert factory_calls[0] > 0


# ---------------------------------------------------------------------------
# Thread safety (basic smoke test)
# ---------------------------------------------------------------------------


@pytest.mark.s3
def test_s3_core_threadsafe(s3_bucket):
    """Multiple threads calling the same cached function all see the same result."""
    results = []

    @cachier(backend="s3", s3_bucket=s3_bucket, s3_region=TEST_REGION)
    def _cached(x):
        return x * 10

    _cached.clear_cache()

    def _call():
        results.append(_cached(3))

    threads = [threading.Thread(target=_call) for _ in range(5)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert all(r == 30 for r in results)


# ---------------------------------------------------------------------------
# Error handling / warnings
# ---------------------------------------------------------------------------


@pytest.mark.s3
def test_s3_bad_bucket_warns():
    """Operations against a non-existent bucket emit a warning rather than crashing."""
    skip_if_missing()
    with mock_aws():
        # Intentionally do NOT create the bucket
        client = boto3.client("s3", region_name=TEST_REGION)

        core = _S3Core(hash_func=None, s3_bucket="nonexistent-bucket", s3_client=client)

        def _dummy(x):
            return x

        core.set_func(_dummy)
        key = core.get_key((), {"x": 1})

        with warnings.catch_warnings(record=True):
            warnings.simplefilter("always")
            result_key, entry = core.get_entry_by_key(key)
        # Should return None (not found / error) without raising
        assert entry is None
        assert result_key == key
