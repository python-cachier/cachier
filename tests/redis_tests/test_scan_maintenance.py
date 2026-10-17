"""Redis maintenance must traverse scan pages without blocking KEYS calls."""

from datetime import datetime, timedelta
from unittest.mock import AsyncMock, NonCallableMock, call

import pytest

from cachier.cores.redis import _RedisCore


def _identity(value: str) -> str:
    """Return a value for cache function identity.

    A Python callable provides the inspectable signature required by the core.

    """
    return value


@pytest.mark.redis
@pytest.mark.parametrize("operation", ["clear_cache", "clear_being_calculated", "delete_stale_entries"])
def test_maintenance_scans_every_page(operation: str) -> None:
    """Visit empty intermediate pages and the final nonempty page.

    Keep deletes and pipeline writes confined to each returned batch.

    """
    pytest.importorskip("redis")
    client = NonCallableMock()
    client.scan.side_effect = [(7, [b"first"]), (19, []), (0, [b"last"])]
    client.keys.side_effect = AssertionError("KEYS must not be used")
    client.hget.return_value = (datetime.now() - timedelta(days=2)).isoformat()
    core = _RedisCore(hash_func=None, redis_client=client)
    core.set_func(_identity)
    args = (timedelta(days=1),) if operation == "delete_stale_entries" else ()
    getattr(core, operation)(*args)
    pattern = f"{core.key_prefix}:{core._func_str}:*"
    assert client.scan.call_args_list == [
        call(cursor=0, match=pattern, count=100),
        call(cursor=7, match=pattern, count=100),
        call(cursor=19, match=pattern, count=100),
    ]
    client.keys.assert_not_called()
    if operation == "clear_being_calculated":
        assert client.pipeline.return_value.hset.call_args_list == [
            call(b"first", "processing", "false"),
            call(b"last", "processing", "false"),
        ]
        assert client.pipeline.return_value.execute.call_count == 2
    else:
        assert client.delete.call_args_list == [call(b"first"), call(b"last")]


@pytest.mark.redis
@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["aclear_cache", "aclear_being_calculated", "adelete_stale_entries"])
async def test_async_maintenance_scans_every_page(operation: str) -> None:
    """Traverse asynchronous scan pages without stopping on an empty page.

    Process the final page before the zero cursor terminates iteration.

    """
    pytest.importorskip("redis")
    client = NonCallableMock()
    client.scan = AsyncMock(side_effect=[(7, [b"first"]), (19, []), (0, [b"last"])])
    client.keys = AsyncMock(side_effect=AssertionError("KEYS must not be used"))
    client.hget = AsyncMock(return_value=(datetime.now() - timedelta(days=2)).isoformat())
    client.hset = AsyncMock()
    client.delete = AsyncMock()
    core = _RedisCore(hash_func=None, redis_client=client)
    core.set_func(_identity)
    args = (timedelta(days=1),) if operation == "adelete_stale_entries" else ()
    await getattr(core, operation)(*args)
    pattern = f"{core.key_prefix}:{core._func_str}:*"
    assert client.scan.await_args_list == [
        call(cursor=0, match=pattern, count=100),
        call(cursor=7, match=pattern, count=100),
        call(cursor=19, match=pattern, count=100),
    ]
    client.keys.assert_not_called()
    if operation == "aclear_being_calculated":
        assert client.hset.await_args_list == [
            call(b"first", "processing", "false"),
            call(b"last", "processing", "false"),
        ]
    else:
        assert client.delete.await_args_list == [call(b"first"), call(b"last")]
