"""A Redis-based caching core for cachier."""

import pickle
import time
import warnings
from contextlib import suppress
from datetime import datetime, timedelta
from typing import Any, Callable, Optional, Tuple, Union

try:
    import redis

    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False

from .._types import HashFunc
from ..config import CacheEntry
from .base import RecalculationNeeded, _BaseCore, _get_func_str

REDIS_SLEEP_DURATION_IN_SEC = 1


class MissingRedisClient(ValueError):
    """Thrown when the redis_client keyword argument is missing."""


class _RedisCore(_BaseCore):
    """Redis-based core for Cachier, supporting Redis backends."""

    def __init__(
        self,
        hash_func: Optional[HashFunc],
        redis_client: Optional[
            Union["redis.Redis", Callable[[], "redis.Redis"]]
        ],
        wait_for_calc_timeout: Optional[int] = None,
        key_prefix: str = "cachier",
        entry_size_limit: Optional[int] = None,
        cache_size_limit: Optional[int] = None,
        replacement_policy: str = "lru",
    ):
        if not REDIS_AVAILABLE:
            warnings.warn(
                "`redis` was not found. Redis cores will not function. "
                "Install with `pip install redis`.",
                ImportWarning,
                stacklevel=2,
            )

        super().__init__(
            hash_func=hash_func,
            wait_for_calc_timeout=wait_for_calc_timeout,
            entry_size_limit=entry_size_limit,
            cache_size_limit=cache_size_limit,
            replacement_policy=replacement_policy,
        )
        if redis_client is None:
            raise MissingRedisClient(
                "must specify ``redis_client`` when using the redis core"
            )
        self.redis_client = redis_client
        self.key_prefix = key_prefix
        self._func_str = None
        self._cache_size_key = None

    def _resolve_redis_client(self):
        """Resolve the Redis client from the provided parameter."""
        if callable(self.redis_client):
            return self.redis_client()
        return self.redis_client

    def _get_redis_key(self, key: str) -> str:
        """Generate a Redis key for the given cache key."""
        return f"{self.key_prefix}:{self._func_str}:{key}"

    def _evict_lru_entries(self, redis_client, current_size: int) -> None:
        """Evict least recently used entries to stay within cache_size_limit.

        Args:
            redis_client: The Redis client instance.
            current_size: The current total cache size in bytes.

        """
        pattern = f"{self.key_prefix}:{self._func_str}:*"

        # Skip special keys like size key
        special_keys: set[str] = (
            {self._cache_size_key} if self._cache_size_key else set()
        )

        # Get all cache keys
        all_keys = []
        for key in redis_client.keys(pattern):
            if key.decode() not in special_keys:
                all_keys.append(key)

        # Get last access times for all entries
        entries_with_access = []
        for key in all_keys:
            try:
                data = redis_client.hmget(key, ["last_access", "size"])
                last_access_str = data[0]
                size_str = data[1]

                if last_access_str and size_str:
                    last_access = datetime.fromisoformat(
                        last_access_str.decode()
                    )
                    size = int(size_str.decode())
                    entries_with_access.append((key, last_access, size))
            except Exception:  # noqa: S112
                # Skip entries that fail to parse
                continue

        # Sort by last access time (oldest first)
        entries_with_access.sort(key=lambda x: x[1])

        # Evict entries until we're under the limit
        evicted_size = 0
        for key, _, size in entries_with_access:
            # Check if we're under the limit (handle None case)
            if (
                self.cache_size_limit is not None
                and current_size - evicted_size <= self.cache_size_limit
            ):
                break

            try:
                # Delete the entry
                redis_client.delete(key)
                evicted_size += size
            except Exception:  # noqa: S112
                # Skip entries that fail to delete
                continue

        # Update the total cache size
        if evicted_size > 0:
            redis_client.decrby(self._cache_size_key, evicted_size)

    def set_func(self, func):
        """Set the function this core will use."""
        super().set_func(func)
        self._func_str = _get_func_str(func)
        self._cache_size_key = f"{self.key_prefix}:{self._func_str}:__size__"

    def get_entry_by_key(self, key: str) -> Tuple[str, Optional[CacheEntry]]:
        """Get entry based on given key from Redis."""
        redis_client = self._resolve_redis_client()
        redis_key = self._get_redis_key(key)

        try:
            # Get the cached data from Redis
            cached_data = redis_client.hgetall(redis_key)
            if not cached_data:
                return key, None

            # Deserialize the value
            value = None
            if cached_data.get(b"value"):
                value = pickle.loads(cached_data[b"value"])

            # Parse timestamp
            timestamp_str = cached_data.get(b"timestamp", b"").decode("utf-8")
            timestamp = (
                datetime.fromisoformat(timestamp_str)
                if timestamp_str
                else datetime.now()
            )

            # Parse boolean fields
            stale = (
                cached_data.get(b"stale", b"false").decode("utf-8").lower()
                == "true"
            )
            processing = (
                cached_data.get(b"processing", b"false")
                .decode("utf-8")
                .lower()
                == "true"
            )
            completed = (
                cached_data.get(b"completed", b"false").decode("utf-8").lower()
                == "true"
            )

            # Update access time for LRU tracking if cache_size_limit is set
            if (
                self.cache_size_limit is not None
                and self.replacement_policy == "lru"
            ):
                redis_client.hset(
                    redis_key, "last_access", datetime.now().isoformat()
                )

            entry = CacheEntry(
                value=value,
                time=timestamp,
                stale=stale,
                _processing=processing,
                _completed=completed,
            )
            return key, entry
        except Exception as e:
            warnings.warn(f"Redis get_entry_by_key failed: {e}", stacklevel=2)
            return key, None

    def set_entry(self, key: str, func_res: Any) -> bool:
        if not self._should_store(func_res):
            return False
        """Map the given result to the given key in Redis."""
        redis_client = self._resolve_redis_client()
        redis_key = self._get_redis_key(key)

        try:
            # Serialize the value
            value_bytes = pickle.dumps(func_res)
            now = datetime.now()
            size = self._estimate_size(func_res)

            # Check if key already exists to update cache size properly
            existing_data = redis_client.hget(redis_key, "value")
            old_size = 0
            if existing_data:
                old_size = self._estimate_size(pickle.loads(existing_data))

            # Store in Redis using hash
            mapping = {
                "value": value_bytes,
                "timestamp": now.isoformat(),
                "last_access": now.isoformat(),
                "stale": "false",
                "processing": "false",
                "completed": "true",
                "size": str(size),
            }
            redis_client.hset(redis_key, mapping=mapping)

            # Update total cache size if cache_size_limit is set
            if self.cache_size_limit is not None:
                # Update cache size atomically
                size_diff = size - old_size
                redis_client.incrby(self._cache_size_key, size_diff)

                # Check if we need to evict entries
                total_size = int(redis_client.get(self._cache_size_key) or 0)
                if total_size > self.cache_size_limit:
                    self._evict_lru_entries(redis_client, total_size)

            return True
        except Exception as e:
            warnings.warn(f"Redis set_entry failed: {e}", stacklevel=2)
        return False

    def mark_entry_being_calculated(self, key: str) -> None:
        """Mark the entry mapped by the given key as being calculated."""
        redis_client = self._resolve_redis_client()
        redis_key = self._get_redis_key(key)

        try:
            now = datetime.now()
            redis_client.hset(
                redis_key,
                mapping={
                    "timestamp": now.isoformat(),
                    "stale": "false",
                    "processing": "true",
                    "completed": "false",
                },
            )
        except Exception as e:
            warnings.warn(
                f"Redis mark_entry_being_calculated failed: {e}", stacklevel=2
            )

    def mark_entry_not_calculated(self, key: str) -> None:
        """Mark the entry mapped by the given key as not being calculated."""
        redis_client = self._resolve_redis_client()
        redis_key = self._get_redis_key(key)

        try:
            redis_client.hset(redis_key, "processing", "false")
        except Exception as e:
            warnings.warn(
                f"Redis mark_entry_not_calculated failed: {e}", stacklevel=2
            )

    def wait_on_entry_calc(self, key: str) -> Any:
        """Wait on the entry with keys being calculated and returns result."""
        time_spent = 0
        while True:
            time.sleep(REDIS_SLEEP_DURATION_IN_SEC)
            time_spent += REDIS_SLEEP_DURATION_IN_SEC
            key, entry = self.get_entry_by_key(key)
            if entry is None:
                raise RecalculationNeeded()
            if not entry._processing:
                return entry.value
            self.check_calc_timeout(time_spent)

    def clear_cache(self) -> None:
        """Clear the cache of this core."""
        redis_client = self._resolve_redis_client()
        pattern = f"{self.key_prefix}:{self._func_str}:*"

        try:
            # Find all keys matching the pattern
            keys = redis_client.keys(pattern)
            if keys:
                redis_client.delete(*keys)
            # Also reset the cache size counter
            if self.cache_size_limit is not None and self._cache_size_key:
                redis_client.delete(self._cache_size_key)
        except Exception as e:
            warnings.warn(f"Redis clear_cache failed: {e}", stacklevel=2)

    def clear_being_calculated(self) -> None:
        """Mark all entries in this cache as not being calculated."""
        redis_client = self._resolve_redis_client()
        pattern = f"{self.key_prefix}:{self._func_str}:*"

        try:
            # Find all keys matching the pattern
            keys = redis_client.keys(pattern)
            if keys:
                # Use pipeline for efficiency
                pipe = redis_client.pipeline()
                for key in keys:
                    pipe.hset(key, "processing", "false")
                pipe.execute()
        except Exception as e:
            warnings.warn(
                f"Redis clear_being_calculated failed: {e}", stacklevel=2
            )

    def delete_stale_entries(self, stale_after: timedelta) -> None:
        """Remove stale entries from the Redis cache."""
        redis_client = self._resolve_redis_client()
        pattern = f"{self.key_prefix}:{self._func_str}:*"
        try:
            keys = redis_client.keys(pattern)
            threshold = datetime.now() - stale_after
            total_deleted_size = 0

            # Skip special keys
            special_keys: set[str] = (
                {self._cache_size_key} if self._cache_size_key else set()
            )

            for key in keys:
                if key.decode() in special_keys:
                    continue

                data = redis_client.hmget(key, ["timestamp", "size"])
                ts = data[0]
                size_str = data[1]

                if ts is None:
                    continue
                try:
                    if isinstance(ts, bytes):
                        ts_str = ts.decode("utf-8")
                    else:
                        ts_str = str(ts)
                    ts_val = datetime.fromisoformat(ts_str)
                except Exception as exc:
                    warnings.warn(
                        f"Redis timestamp parse failed: {exc}", stacklevel=2
                    )
                    continue
                if ts_val < threshold:
                    # Track size before deleting
                    if self.cache_size_limit is not None and size_str:
                        with suppress(Exception):
                            total_deleted_size += int(size_str.decode())
                    redis_client.delete(key)

            # Update cache size if needed
            if self.cache_size_limit is not None and total_deleted_size > 0:
                redis_client.decrby(self._cache_size_key, total_deleted_size)

        except Exception as e:
            warnings.warn(
                f"Redis delete_stale_entries failed: {e}", stacklevel=2
            )
