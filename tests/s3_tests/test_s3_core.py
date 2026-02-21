"""Tests for the S3 caching core."""

import asyncio
import contextlib
import threading
import warnings
from datetime import datetime, timedelta
from random import random
from unittest.mock import Mock

import pytest

from cachier.config import CacheEntry
from cachier.cores.base import RecalculationNeeded
from tests.s3_tests.helpers import S3_DEPS_AVAILABLE, TEST_BUCKET, TEST_REGION, skip_if_missing

if S3_DEPS_AVAILABLE:
    import boto3
    import botocore
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
@pytest.mark.asyncio
async def test_s3_core_async_cache_hit(s3_bucket):
    """Async S3 cache calls should hit cache on repeated arguments."""
    call_count = [0]

    @cachier(backend="s3", s3_bucket=s3_bucket, s3_region=TEST_REGION)
    async def _cached(x):
        call_count[0] += 1
        await asyncio.sleep(0)
        return x * 5

    await _cached.clear_cache()
    value_1 = await _cached(6)
    value_2 = await _cached(6)

    assert value_1 == 30
    assert value_2 == 30
    assert call_count[0] == 1


@pytest.mark.s3
@pytest.mark.asyncio
async def test_s3_core_async_get_entry_by_key_missing(s3_bucket):
    """aget_entry_by_key delegates correctly and returns missing entries as None."""
    skip_if_missing()
    core = _make_core(s3_bucket=s3_bucket)

    def _dummy(x):
        return x

    core.set_func(_dummy)
    key = core.get_key((), {"x": 123})
    returned_key, entry = await core.aget_entry_by_key(key)
    assert returned_key == key
    assert entry is None


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


@pytest.mark.s3
def test_core_decorator_constructs_s3_core(monkeypatch):
    """The public decorator routes backend='s3' through the _S3Core branch."""
    skip_if_missing()
    import cachier.core as core_mod

    captured = {}

    class DummyS3Core:
        """Minimal drop-in core used to verify constructor wiring."""

        def __init__(self, **kwargs):
            captured.update(kwargs)

        def set_func(self, func):
            self.func = func
            self.func_is_method = False

        def get_key(self, args, kwds):
            return "dummy-key"

        def get_entry(self, args, kwds):
            return "dummy-key", None

        def set_entry(self, key, func_res):
            return True

        def mark_entry_being_calculated(self, key):
            return None

        def mark_entry_not_calculated(self, key):
            return None

        def wait_on_entry_calc(self, key):
            return 42

        def clear_cache(self):
            return None

        def clear_being_calculated(self):
            return None

        def delete_stale_entries(self, stale_after):
            return None

    monkeypatch.setattr(core_mod, "_S3Core", DummyS3Core)

    @core_mod.cachier(backend="s3", s3_bucket="bucket", s3_prefix="prefix")
    def decorated(x):
        return x

    assert decorated(3) == 3
    assert captured["s3_bucket"] == "bucket"
    assert captured["s3_prefix"] == "prefix"


@pytest.mark.s3
def test_s3_internal_helpers_and_error_paths(monkeypatch):
    """Exercise internal helper branches and warning paths."""
    skip_if_missing()

    class DummyFactoryClient:
        pass

    factory_client = DummyFactoryClient()

    core = _S3Core(
        hash_func=None,
        s3_bucket=TEST_BUCKET,
        s3_prefix="my-prefix",
        s3_client_factory=lambda: factory_client,
    )

    def _dummy(x):
        return x

    core.set_func(_dummy)

    assert core._get_s3_client() is factory_client
    assert core._get_s3_key("abc") == "my-prefix/.tests.s3_tests.test_s3_core._dummy/abc.pkl"
    assert core._get_s3_prefix() == "my-prefix/.tests.s3_tests.test_s3_core._dummy/"

    bad_pickle = b"not a pickle"
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        assert core._load_entry(bad_pickle) is None
    assert any("deserialization failed" in str(w.message) for w in caught)

    invalid_data = __import__("pickle").dumps(1)
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        assert core._load_entry(invalid_data) is None
    assert any("CacheEntry construction failed" in str(w.message) for w in caught)


@pytest.mark.s3
def test_s3_get_client_builds_boto3_client(monkeypatch):
    """_get_s3_client forwards region/endpoint/config kwargs when auto-creating."""
    skip_if_missing()
    import cachier.cores.s3 as s3_mod

    created = {}

    def fake_client(name, **kwargs):
        created["name"] = name
        created["kwargs"] = kwargs
        return "fake-client"

    monkeypatch.setattr(s3_mod.boto3, "client", fake_client)

    core = _S3Core(
        hash_func=None,
        s3_bucket=TEST_BUCKET,
        s3_region="us-west-2",
        s3_endpoint_url="http://localhost:9000",
        s3_config=object(),
    )

    assert core._get_s3_client() == "fake-client"
    assert created["name"] == "s3"
    assert set(created["kwargs"]) == {"region_name", "endpoint_url", "config"}

    created.clear()
    no_options_core = _S3Core(hash_func=None, s3_bucket=TEST_BUCKET)
    assert no_options_core._get_s3_client() == "fake-client"
    assert created["kwargs"] == {}


@pytest.mark.s3
def test_s3_get_entry_by_key_branches(monkeypatch):
    """get_entry_by_key handles success and exception branches."""
    skip_if_missing()

    core = _make_core(s3_client=Mock())

    def _dummy(x):
        return x

    core.set_func(_dummy)

    payload = core._dump_entry(
        CacheEntry(
            value=5,
            time=datetime.now(),
            stale=False,
            _processing=False,
            _completed=True,
        )
    )
    good_body = Mock(read=Mock(return_value=payload))
    core._s3_client.get_object = Mock(return_value={"Body": good_body})
    _, entry = core.get_entry_by_key("k")
    assert entry is not None
    assert entry.value == 5

    client_error = botocore.exceptions.ClientError(
        {"Error": {"Code": "NoSuchKey", "Message": "missing"}},
        "GetObject",
    )
    core._s3_client.get_object = Mock(side_effect=client_error)
    assert core.get_entry_by_key("k") == ("k", None)

    bad_client_error = botocore.exceptions.ClientError(
        {"Error": {"Code": "AccessDenied", "Message": "denied"}},
        "GetObject",
    )
    core._s3_client.get_object = Mock(side_effect=bad_client_error)
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        assert core.get_entry_by_key("k") == ("k", None)
    assert any("get_entry_by_key failed" in str(w.message) for w in caught)

    core._s3_client.get_object = Mock(side_effect=RuntimeError("boom"))
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        assert core.get_entry_by_key("k") == ("k", None)
    assert any("get_entry_by_key failed" in str(w.message) for w in caught)


@pytest.mark.s3
def test_s3_set_mark_wait_and_clear_paths(monkeypatch):
    """Exercise set/mark/wait and clear path branches."""
    skip_if_missing()
    import datetime as dt

    client = Mock()
    core = _make_core(s3_client=client)

    def _dummy(x):
        return x

    core.set_func(_dummy)

    monkeypatch.setattr(core, "_should_store", lambda _: False)
    assert core.set_entry("k", 1) is False

    monkeypatch.setattr(core, "_should_store", lambda _: True)
    client.put_object = Mock(return_value=None)
    assert core.set_entry("k", 1) is True

    client.put_object = Mock(side_effect=RuntimeError("put failed"))
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        assert core.set_entry("k", 1) is False
    assert any("set_entry failed" in str(w.message) for w in caught)

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        core.mark_entry_being_calculated("k")
    assert any("mark_entry_being_calculated failed" in str(w.message) for w in caught)

    entry = CacheEntry(value=3, time=dt.datetime.now(), stale=False, _processing=True, _completed=False)
    client.get_object = Mock(return_value={"Body": Mock(read=Mock(return_value=core._dump_entry(entry)))})
    client.put_object = Mock(return_value=None)
    core.mark_entry_not_calculated("k")
    assert client.put_object.called

    no_such_key = botocore.exceptions.ClientError({"Error": {"Code": "NoSuchKey"}}, "GetObject")
    client.get_object = Mock(side_effect=no_such_key)
    core.mark_entry_not_calculated("k")

    denied = botocore.exceptions.ClientError({"Error": {"Code": "AccessDenied"}}, "GetObject")
    client.get_object = Mock(side_effect=denied)
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        core.mark_entry_not_calculated("k")
    assert any("mark_entry_not_calculated failed" in str(w.message) for w in caught)

    client.get_object = Mock(return_value={"Body": Mock(read=Mock(return_value=b"bad"))})
    with warnings.catch_warnings(record=True):
        warnings.simplefilter("always")
        core.mark_entry_not_calculated("k")

    client.get_object = Mock(side_effect=RuntimeError("unexpected"))
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        core.mark_entry_not_calculated("k")
    assert any("mark_entry_not_calculated failed" in str(w.message) for w in caught)

    monkeypatch.setattr("cachier.cores.s3.time.sleep", lambda _: None)
    sequence = [
        ("k", CacheEntry(value=None, time=dt.datetime.now(), stale=False, _processing=True, _completed=False)),
        ("k", CacheEntry(value=9, time=dt.datetime.now(), stale=False, _processing=False, _completed=True)),
    ]
    monkeypatch.setattr(core, "get_entry_by_key", lambda key: sequence.pop(0))
    assert core.wait_on_entry_calc("k") == 9

    monkeypatch.setattr(core, "get_entry_by_key", lambda key: ("k", None))
    with pytest.raises(RecalculationNeeded):
        core.wait_on_entry_calc("k")

    paginator = Mock()
    paginator.paginate.return_value = [{"Contents": [{"Key": f"k{i}"} for i in range(1001)]}]
    client.get_paginator = Mock(return_value=paginator)
    client.delete_objects = Mock(return_value=None)
    core.clear_cache()
    assert client.delete_objects.call_count == 2

    client.get_paginator = Mock(side_effect=RuntimeError("paginate failed"))
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        core.clear_cache()
    assert any("clear_cache failed" in str(w.message) for w in caught)


@pytest.mark.s3
def test_s3_clear_processing_and_delete_stale_paths(monkeypatch):
    """Exercise clear_being_calculated and delete_stale_entries branches."""
    skip_if_missing()
    import datetime as dt

    client = Mock()
    core = _make_core(s3_client=client)

    def _dummy(x):
        return x

    core.set_func(_dummy)

    now = dt.datetime.now()
    processing = CacheEntry(value=1, time=now, stale=False, _processing=True, _completed=False)
    done = CacheEntry(value=2, time=now, stale=False, _processing=False, _completed=True)

    paginator = Mock()
    paginator.paginate.return_value = [{"Contents": [{"Key": "k1"}, {"Key": "k2"}]}]
    client.get_paginator = Mock(return_value=paginator)
    client.get_object = Mock(
        side_effect=[
            {"Body": Mock(read=Mock(return_value=core._dump_entry(processing)))},
            {"Body": Mock(read=Mock(return_value=core._dump_entry(done)))},
        ]
    )
    client.put_object = Mock(return_value=None)
    core.clear_being_calculated()
    assert client.put_object.call_count == 1

    client.get_paginator = Mock(return_value=paginator)
    client.get_object = Mock(side_effect=RuntimeError("entry read failed"))
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        core.clear_being_calculated()
    assert any("clear_being_calculated entry update failed" in str(w.message) for w in caught)

    client.get_paginator = Mock(side_effect=RuntimeError("outer failed"))
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        core.clear_being_calculated()
    assert any("clear_being_calculated failed" in str(w.message) for w in caught)

    old = CacheEntry(value=1, time=now - dt.timedelta(days=2), stale=False, _processing=False, _completed=True)
    fresh = CacheEntry(value=2, time=now, stale=False, _processing=False, _completed=True)
    client.get_paginator = Mock(return_value=paginator)
    client.get_object = Mock(
        side_effect=[
            {"Body": Mock(read=Mock(return_value=core._dump_entry(old)))},
            {"Body": Mock(read=Mock(return_value=core._dump_entry(fresh)))},
        ]
    )
    client.delete_object = Mock(return_value=None)
    core.delete_stale_entries(dt.timedelta(days=1))
    assert client.delete_object.call_count == 1

    client.get_paginator = Mock(return_value=paginator)
    client.get_object = Mock(side_effect=RuntimeError("entry read failed"))
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        core.delete_stale_entries(dt.timedelta(days=1))
    assert any("delete_stale_entries entry check failed" in str(w.message) for w in caught)

    client.get_paginator = Mock(side_effect=RuntimeError("outer failed"))
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        core.delete_stale_entries(dt.timedelta(days=1))
    assert any("delete_stale_entries failed" in str(w.message) for w in caught)


@pytest.mark.s3
def test_s3_module_importerror_branch(monkeypatch):
    """The module sets BOTO3_AVAILABLE=False when boto3 import fails."""
    import builtins
    import importlib.util
    from pathlib import Path

    source = Path("src/cachier/cores/s3.py")
    spec = importlib.util.spec_from_file_location("cachier.cores.s3_no_boto", source)
    module = importlib.util.module_from_spec(spec)
    original_import = builtins.__import__

    def fake_import(name, *args, **kwargs):
        if name in {"boto3", "botocore.exceptions"}:
            raise ImportError("missing for test")
        return original_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", fake_import)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    assert module.BOTO3_AVAILABLE is False
