"""A Redis-based caching core for cachier."""

import pickle
import time
import warnings
from datetime import datetime
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
    ):
        if not REDIS_AVAILABLE:
            warnings.warn(
                "`redis` was not found. Redis cores will not function. "
                "Install with `pip install redis`.",
                ImportWarning,
                stacklevel=2,
            )

        super().__init__(
            hash_func=hash_func, wait_for_calc_timeout=wait_for_calc_timeout
        )
        if redis_client is None:
            raise MissingRedisClient(
                "must specify ``redis_client`` when using the redis core"
            )
        self.redis_client = redis_client
        self.key_prefix = key_prefix
        self._func_str = None

    def _resolve_redis_client(self):
        """Resolve the Redis client from the provided parameter."""
        if callable(self.redis_client):
            return self.redis_client()
        return self.redis_client

    def _get_redis_key(self, key: str) -> str:
        """Generate a Redis key for the given cache key."""
        return f"{self.key_prefix}:{self._func_str}:{key}"

    def set_func(self, func):
        """Set the function this core will use."""
        super().set_func(func)
        self._func_str = _get_func_str(func)

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

    def set_entry(self, key: str, func_res: Any) -> None:
        """Map the given result to the given key in Redis."""
        redis_client = self._resolve_redis_client()
        redis_key = self._get_redis_key(key)

        try:
            # Serialize the value
            value_bytes = pickle.dumps(func_res)
            now = datetime.now()

            # Store in Redis using hash
            redis_client.hset(
                redis_key,
                mapping={
                    "value": value_bytes,
                    "timestamp": now.isoformat(),
                    "stale": "false",
                    "processing": "false",
                    "completed": "true",
                },
            )
        except Exception as e:
            warnings.warn(f"Redis set_entry failed: {e}", stacklevel=2)

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
