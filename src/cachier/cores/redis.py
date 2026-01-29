"""A Redis-based caching core for cachier."""

import pickle
import time
import warnings
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any, Callable, Optional, Tuple, Union

try:
    import redis

    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False

from .._types import HashFunc
from ..config import CacheEntry
from .base import RecalculationNeeded, _BaseCore, _get_func_str

if TYPE_CHECKING:
    from ..metrics import CacheMetrics

REDIS_SLEEP_DURATION_IN_SEC = 1


class MissingRedisClient(ValueError):
    """Thrown when the redis_client keyword argument is missing."""


class _RedisCore(_BaseCore):
    """Redis-based core for Cachier, supporting Redis backends."""

    def __init__(
        self,
        hash_func: Optional[HashFunc],
        redis_client: Optional[Union["redis.Redis", Callable[[], "redis.Redis"]]],
        wait_for_calc_timeout: Optional[int] = None,
        key_prefix: str = "cachier",
        entry_size_limit: Optional[int] = None,
        metrics: Optional["CacheMetrics"] = None,
    ):
        if not REDIS_AVAILABLE:
            warnings.warn(
                "`redis` was not found. Redis cores will not function. Install with `pip install redis`.",
                ImportWarning,
                stacklevel=2,
            )

        super().__init__(
            hash_func=hash_func,
            wait_for_calc_timeout=wait_for_calc_timeout,
            entry_size_limit=entry_size_limit,
            metrics=metrics
        )
        if redis_client is None:
            raise MissingRedisClient("must specify ``redis_client`` when using the redis core")
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

    @staticmethod
    def _loading_pickle(raw_value) -> Any:
        """Load pickled data with some recovery attempts."""
        try:
            if isinstance(raw_value, bytes):
                return pickle.loads(raw_value)
            elif isinstance(raw_value, str):
                # try to recover by encoding; prefer utf-8 but fall
                # back to latin-1 in case raw binary was coerced to str
                try:
                    return pickle.loads(raw_value.encode("utf-8"))
                except Exception:
                    return pickle.loads(raw_value.encode("latin-1"))
            else:
                # unexpected type; attempt pickle.loads directly
                try:
                    return pickle.loads(raw_value)
                except Exception:
                    return None
        except Exception as exc:
            warnings.warn(
                f"Redis value deserialization failed: {exc}",
                stacklevel=2,
            )
        return None

    @staticmethod
    def _get_raw_field(cached_data, field: str):
        """Fetch field from cached_data with bytes/str key handling."""
        # try bytes key first, then str key
        bkey = field.encode("utf-8")
        if bkey in cached_data:
            return cached_data[bkey]
        return cached_data.get(field)

    @staticmethod
    def _get_bool_field(cached_data, name: str) -> bool:
        """Fetch boolean field from cached_data."""
        raw = _RedisCore._get_raw_field(cached_data, name) or b"false"
        if isinstance(raw, bytes):
            try:
                s = raw.decode("utf-8")
            except Exception:
                s = raw.decode("latin-1", errors="ignore")
        else:
            s = str(raw)
        return s.lower() == "true"

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
            raw_value = _RedisCore._get_raw_field(cached_data, "value")
            if raw_value is not None:
                value = self._loading_pickle(raw_value)

            # Parse timestamp
            raw_ts = _RedisCore._get_raw_field(cached_data, "timestamp") or b""
            if isinstance(raw_ts, bytes):
                try:
                    timestamp_str = raw_ts.decode("utf-8")
                except UnicodeDecodeError:
                    timestamp_str = raw_ts.decode("latin-1", errors="ignore")
            else:
                timestamp_str = str(raw_ts)
            timestamp = datetime.fromisoformat(timestamp_str) if timestamp_str else datetime.now()

            stale = _RedisCore._get_bool_field(cached_data, "stale")
            processing = _RedisCore._get_bool_field(cached_data, "processing")
            completed = _RedisCore._get_bool_field(cached_data, "completed")

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
        """Map the given result to the given key in Redis."""
        if not self._should_store(func_res):
            return False
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
                mapping={"timestamp": now.isoformat(), "stale": "false", "processing": "true", "completed": "false"},
            )
        except Exception as e:
            warnings.warn(f"Redis mark_entry_being_calculated failed: {e}", stacklevel=2)

    def mark_entry_not_calculated(self, key: str) -> None:
        """Mark the entry mapped by the given key as not being calculated."""
        redis_client = self._resolve_redis_client()
        redis_key = self._get_redis_key(key)

        try:
            redis_client.hset(redis_key, "processing", "false")
        except Exception as e:
            warnings.warn(f"Redis mark_entry_not_calculated failed: {e}", stacklevel=2)

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
            warnings.warn(f"Redis clear_being_calculated failed: {e}", stacklevel=2)

    def delete_stale_entries(self, stale_after: timedelta) -> None:
        """Remove stale entries from the Redis cache."""
        redis_client = self._resolve_redis_client()
        pattern = f"{self.key_prefix}:{self._func_str}:*"
        try:
            keys = redis_client.keys(pattern)
            threshold = datetime.now() - stale_after
            for key in keys:
                ts = redis_client.hget(key, "timestamp")
                if ts is None:
                    continue
                # ts may be bytes or str depending on client configuration
                if isinstance(ts, bytes):
                    try:
                        ts_s = ts.decode("utf-8")
                    except Exception:
                        ts_s = ts.decode("latin-1", errors="ignore")
                else:
                    ts_s = str(ts)
                try:
                    ts_val = datetime.fromisoformat(ts_s)
                except Exception as exc:
                    warnings.warn(f"Redis timestamp parse failed: {exc}", stacklevel=2)
                    continue
                if ts_val < threshold:
                    redis_client.delete(key)
        except Exception as e:
            warnings.warn(f"Redis delete_stale_entries failed: {e}", stacklevel=2)
