"""Tests for async Redis client with async functions."""

import asyncio
from datetime import datetime, timedelta
from fnmatch import fnmatch
from random import random

import pytest

from cachier import cachier
from cachier.cores.redis import _RedisCore


class _AsyncInMemoryRedis:
    """Minimal async Redis-like client implementing required hash operations."""

    def __init__(self):
        self._data: dict[str, dict[str, object]] = {}
        self.fail_hgetall = False
        self.fail_hset = False
        self.fail_keys = False
        self.fail_delete = False
        self.fail_hget = False

    async def hgetall(self, key: str) -> dict[bytes, object]:
        if self.fail_hgetall:
            raise Exception("hgetall failed")
        raw = self._data.get(key, {})
        res: dict[bytes, object] = {}
        for k, v in raw.items():
            res[k.encode("utf-8")] = v.encode("utf-8") if isinstance(v, str) else v
        return res

    async def hset(self, key: str, field=None, value=None, mapping=None, **kwargs):
        if self.fail_hset:
            raise Exception("hset failed")
        if key not in self._data:
            self._data[key] = {}

        if mapping is not None:
            self._data[key].update(mapping)
            return
        if field is not None and value is not None:
            self._data[key][field] = value
            return
        if kwargs:
            self._data[key].update(kwargs)

    async def keys(self, pattern: str) -> list[str]:
        if self.fail_keys:
            raise Exception("keys failed")
        return [key for key in self._data if fnmatch(key, pattern)]

    async def delete(self, *keys: str):
        if self.fail_delete:
            raise Exception("delete failed")
        for key in keys:
            self._data.pop(key, None)

    async def hget(self, key: str, field: str):
        if self.fail_hget:
            raise Exception("hget failed")
        return self._data.get(key, {}).get(field)


class _SyncInMemoryRedis:
    """Minimal sync Redis-like client exposing required hash operations."""

    def hgetall(self, key: str) -> dict[bytes, object]:
        return {}

    def hset(self, key: str, field=None, value=None, mapping=None, **kwargs):
        return None

    def keys(self, pattern: str) -> list[str]:
        return []

    def delete(self, *keys: str):
        return None

    def hget(self, key: str, field: str):
        return None


@pytest.mark.redis
@pytest.mark.asyncio
async def test_async_redis_requires_async_client_callable():
    pytest.importorskip("redis")

    def get_sync_client():
        return _AsyncInMemoryRedis()

    with pytest.raises(
        TypeError,
        match="Async cached functions with Redis backend require an async redis_client callable.",
    ):

        @cachier(backend="redis", redis_client=get_sync_client)
        async def async_cached_redis_requires_async_callable(_: int) -> int:
            return 1


@pytest.mark.redis
@pytest.mark.asyncio
async def test_async_redis_requires_async_client_instance():
    pytest.importorskip("redis")

    with pytest.raises(
        TypeError,
        match="Async cached functions with Redis backend require an async Redis client.",
    ):

        @cachier(backend="redis", redis_client=_SyncInMemoryRedis())
        async def async_cached_redis_requires_async_client(_: int) -> int:
            return 1


@pytest.mark.redis
def test_async_redis_rejects_async_client_callable_for_sync_function():
    pytest.importorskip("redis")

    async def get_async_client():
        return _AsyncInMemoryRedis()

    with pytest.raises(
        TypeError,
        match="Async redis_client callable requires an async cached function.",
    ):

        @cachier(backend="redis", redis_client=get_async_client)
        def sync_cached_redis_requires_sync_callable(_: int) -> int:
            return 1


@pytest.mark.redis
def test_async_redis_rejects_async_client_instance_for_sync_function():
    pytest.importorskip("redis")

    with pytest.raises(
        TypeError,
        match="Async Redis client requires an async cached function.",
    ):

        @cachier(backend="redis", redis_client=_AsyncInMemoryRedis())
        def sync_cached_redis_requires_sync_client(_: int) -> int:
            return 1


@pytest.mark.redis
@pytest.mark.asyncio
async def test_async_redis_client_factory():
    pytest.importorskip("redis")

    client = _AsyncInMemoryRedis()

    async def get_redis_client():
        return client

    @cachier(backend="redis", redis_client=get_redis_client)
    async def async_cached_redis(x: int) -> float:
        await asyncio.sleep(0.01)
        return random() + x

    val1 = await async_cached_redis(3)
    val2 = await async_cached_redis(3)
    assert val1 == val2


@pytest.mark.redis
@pytest.mark.asyncio
async def test_async_redis_client_factory_method_args_and_kwargs():
    pytest.importorskip("redis")

    client = _AsyncInMemoryRedis()

    async def get_redis_client():
        return client

    call_count = 0

    class _RedisMethods:
        @cachier(backend="redis", redis_client=get_redis_client)
        async def async_cached_redis_method_args_kwargs(self, x: int, y: int) -> int:
            nonlocal call_count
            call_count += 1
            await asyncio.sleep(0.01)
            return call_count

    obj = _RedisMethods()
    val1 = await obj.async_cached_redis_method_args_kwargs(1, 2)
    val2 = await obj.async_cached_redis_method_args_kwargs(y=2, x=1)
    assert val1 == val2 == 1
    assert call_count == 1


def _build_async_core(client: _AsyncInMemoryRedis) -> _RedisCore:
    """Build a Redis core configured for async tests."""
    core = _RedisCore(hash_func=None, redis_client=client, wait_for_calc_timeout=10)

    def _func(x: int) -> int:
        return x

    core.set_func(_func)
    return core


@pytest.mark.redis
@pytest.mark.asyncio
async def test_async_redis_core_helpers_and_client_resolution():
    pytest.importorskip("redis")

    assert _RedisCore._get_bool_field({"processing": "true"}, "processing") is True

    client = _AsyncInMemoryRedis()
    core = _build_async_core(client)
    assert await core._resolve_redis_client_async() is client

    async def async_client_factory():
        return client

    core_with_async_factory = _RedisCore(
        hash_func=None,
        redis_client=async_client_factory,
        wait_for_calc_timeout=10,
    )
    core_with_async_factory.set_func(lambda x: x)
    assert await core_with_async_factory._resolve_redis_client_async() is client


@pytest.mark.redis
@pytest.mark.asyncio
async def test_async_redis_core_entry_read_write_paths():
    pytest.importorskip("redis")

    client = _AsyncInMemoryRedis()
    core = _build_async_core(client)

    assert await core.aset_entry("k1", {"value": 1}) is True
    _, entry = await core.aget_entry_by_key("k1")
    assert entry is not None
    assert entry.value == {"value": 1}

    _, none_entry = await core.aget_entry_by_key("missing")
    assert none_entry is None

    redis_key = core._get_redis_key("invalid-ts")
    await client.hset(
        redis_key,
        mapping={
            "timestamp": b"\xff",
            "stale": "false",
            "processing": "false",
            "completed": "true",
        },
    )
    with pytest.warns(UserWarning, match="Redis get_entry_by_key failed"):
        result = await core.aget_entry_by_key("invalid-ts")
    assert result[1] is None

    redis_key_nonbytes = core._get_redis_key("nonbytes-ts")
    await client.hset(
        redis_key_nonbytes,
        mapping={
            "timestamp": 12345,
            "stale": "false",
            "processing": "false",
            "completed": "true",
        },
    )
    with pytest.warns(UserWarning, match="Redis get_entry_by_key failed"):
        nonbytes_result = await core.aget_entry_by_key("nonbytes-ts")
    assert nonbytes_result[1] is None


@pytest.mark.redis
@pytest.mark.asyncio
async def test_async_redis_core_entry_write_exceptions_and_should_store_guard():
    pytest.importorskip("redis")

    client = _AsyncInMemoryRedis()
    core = _build_async_core(client)

    core._should_store = lambda _value: False
    assert await core.aset_entry("ignored", None) is False

    failing_client = _AsyncInMemoryRedis()
    failing_client.fail_hset = True
    failing_core = _build_async_core(failing_client)
    with pytest.warns(UserWarning, match="Redis set_entry failed"):
        assert await failing_core.aset_entry("k2", "v2") is False


@pytest.mark.redis
@pytest.mark.asyncio
async def test_async_redis_core_mark_and_clear_paths():
    pytest.importorskip("redis")

    client = _AsyncInMemoryRedis()
    core = _build_async_core(client)

    await core.amark_entry_being_calculated("calc")
    _, entry = await core.aget_entry_by_key("calc")
    assert entry is not None
    assert entry._processing is True

    await core.amark_entry_not_calculated("calc")
    _, entry = await core.aget_entry_by_key("calc")
    assert entry is not None
    assert entry._processing is False

    await core.aset_entry("cleanup-1", 1)
    await core.aset_entry("cleanup-2", 2)
    await core.aclear_being_calculated()

    _, clean1 = await core.aget_entry_by_key("cleanup-1")
    _, clean2 = await core.aget_entry_by_key("cleanup-2")
    assert clean1 is not None
    assert clean2 is not None
    assert clean1._processing is False
    assert clean2._processing is False

    await core.aclear_cache()
    _, cleared = await core.aget_entry_by_key("cleanup-1")
    assert cleared is None

    await core.aclear_cache()


@pytest.mark.redis
@pytest.mark.asyncio
async def test_async_redis_core_mark_and_clear_exceptions():
    pytest.importorskip("redis")

    client = _AsyncInMemoryRedis()
    core = _build_async_core(client)

    client.fail_hset = True
    with pytest.warns(UserWarning, match="Redis mark_entry_being_calculated failed"):
        await core.amark_entry_being_calculated("k")
    with pytest.warns(UserWarning, match="Redis mark_entry_not_calculated failed"):
        await core.amark_entry_not_calculated("k")
    client.fail_hset = False
    client.fail_keys = True
    with pytest.warns(UserWarning, match="Redis clear_being_calculated failed"):
        await core.aclear_being_calculated()

    with pytest.warns(UserWarning, match="Redis clear_cache failed"):
        await core.aclear_cache()


@pytest.mark.redis
@pytest.mark.asyncio
async def test_async_redis_core_delete_stale_entries_paths():
    pytest.importorskip("redis")

    client = _AsyncInMemoryRedis()
    core = _build_async_core(client)

    now = datetime.now()
    stale_ts = (now - timedelta(hours=2)).isoformat()
    fresh_ts = (now - timedelta(minutes=10)).isoformat()

    stale_key = core._get_redis_key("stale")
    fresh_key = core._get_redis_key("fresh")
    bad_key = core._get_redis_key("bad-ts")
    bad_bytes_key = core._get_redis_key("bad-bytes-ts")
    none_key = core._get_redis_key("none-ts")

    await client.hset(stale_key, mapping={"timestamp": stale_ts, "processing": "false", "completed": "true"})
    await client.hset(fresh_key, mapping={"timestamp": fresh_ts, "processing": "false", "completed": "true"})
    await client.hset(bad_key, mapping={"timestamp": "invalid", "processing": "false", "completed": "true"})
    await client.hset(bad_bytes_key, mapping={"timestamp": b"\xff", "processing": "false", "completed": "true"})
    await client.hset(none_key, mapping={"processing": "false", "completed": "true"})

    with pytest.warns(UserWarning, match="Redis timestamp parse failed"):
        await core.adelete_stale_entries(timedelta(hours=1))

    _, stale_entry = await core.aget_entry_by_key("stale")
    _, fresh_entry = await core.aget_entry_by_key("fresh")
    assert stale_entry is None
    assert fresh_entry is not None

    client.fail_keys = True
    with pytest.warns(UserWarning, match="Redis delete_stale_entries failed"):
        await core.adelete_stale_entries(timedelta(hours=1))
