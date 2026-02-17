"""An S3-based caching core for cachier."""

import pickle
import time
import warnings
from datetime import datetime, timedelta
from typing import Any, Callable, Optional, Tuple

try:
    import boto3  # type: ignore[import-untyped]
    import botocore.exceptions  # type: ignore[import-untyped]

    BOTO3_AVAILABLE = True
except ImportError:
    BOTO3_AVAILABLE = False

from .._types import HashFunc
from ..config import CacheEntry
from .base import RecalculationNeeded, _BaseCore, _get_func_str

S3_SLEEP_DURATION_IN_SEC = 1


class MissingS3Bucket(ValueError):
    """Thrown when the s3_bucket keyword argument is missing."""


class _S3Core(_BaseCore):
    """S3-based core for Cachier, supporting AWS S3 and S3-compatible backends.

    Parameters
    ----------
    hash_func : callable, optional
        A callable to hash function arguments into a cache key string.
    s3_bucket : str
        The name of the S3 bucket to use for caching.
    wait_for_calc_timeout : int, optional
        Maximum seconds to wait for a concurrent calculation. 0 means wait forever.
    s3_prefix : str, optional
        Key prefix for all cache entries. Defaults to ``"cachier"``.
    s3_client : boto3 S3 client, optional
        A pre-configured boto3 S3 client instance.
    s3_client_factory : callable, optional
        A callable that returns a boto3 S3 client. Allows lazy initialization.
    s3_region : str, optional
        AWS region name used when creating the S3 client.
    s3_endpoint_url : str, optional
        Custom endpoint URL for S3-compatible services (e.g. MinIO, localstack).
    s3_config : boto3 Config, optional
        Optional ``botocore.config.Config`` object passed when creating the client.
    entry_size_limit : int, optional
        Maximum allowed size in bytes of a cached value.

    """

    def __init__(
        self,
        hash_func: Optional[HashFunc],
        s3_bucket: Optional[str],
        wait_for_calc_timeout: Optional[int] = None,
        s3_prefix: str = "cachier",
        s3_client: Optional[Any] = None,
        s3_client_factory: Optional[Callable[[], Any]] = None,
        s3_region: Optional[str] = None,
        s3_endpoint_url: Optional[str] = None,
        s3_config: Optional[Any] = None,
        entry_size_limit: Optional[int] = None,
    ):
        if not BOTO3_AVAILABLE:
            warnings.warn(
                "`boto3` was not found. S3 cores will not function. Install with `pip install boto3`.",
                ImportWarning,
                stacklevel=2,
            )

        super().__init__(
            hash_func=hash_func,
            wait_for_calc_timeout=wait_for_calc_timeout,
            entry_size_limit=entry_size_limit,
        )

        if not s3_bucket:
            raise MissingS3Bucket("must specify ``s3_bucket`` when using the s3 core")

        self.s3_bucket = s3_bucket
        self.s3_prefix = s3_prefix
        self._s3_client = s3_client
        self._s3_client_factory = s3_client_factory
        self._s3_region = s3_region
        self._s3_endpoint_url = s3_endpoint_url
        self._s3_config = s3_config
        self._func_str: Optional[str] = None

    def set_func(self, func: Callable) -> None:
        """Set the function this core will use."""
        super().set_func(func)
        self._func_str = _get_func_str(func)

    def _get_s3_client(self) -> Any:
        """Return a boto3 S3 client, creating one if not already available."""
        if self._s3_client_factory is not None:
            return self._s3_client_factory()
        if self._s3_client is not None:
            return self._s3_client
        kwargs: dict = {}
        if self._s3_region:
            kwargs["region_name"] = self._s3_region
        if self._s3_endpoint_url:
            kwargs["endpoint_url"] = self._s3_endpoint_url
        if self._s3_config:
            kwargs["config"] = self._s3_config
        self._s3_client = boto3.client("s3", **kwargs)
        return self._s3_client

    def _get_s3_key(self, key: str) -> str:
        """Return the full S3 object key for the given cache key."""
        return f"{self.s3_prefix}/{self._func_str}/{key}.pkl"

    def _get_s3_prefix(self) -> str:
        """Return the S3 prefix for all objects belonging to this function."""
        return f"{self.s3_prefix}/{self._func_str}/"

    def _load_entry(self, body: bytes) -> Optional[CacheEntry]:
        """Deserialize raw S3 object bytes into a CacheEntry."""
        try:
            data = pickle.loads(body)
        except Exception as exc:
            warnings.warn(f"S3 cache entry deserialization failed: {exc}", stacklevel=2)
            return None

        try:
            raw_time = data.get("time", datetime.now())
            entry_time = datetime.fromisoformat(raw_time) if isinstance(raw_time, str) else raw_time

            return CacheEntry(
                value=data.get("value"),
                time=entry_time,
                stale=bool(data.get("stale", False)),
                _processing=bool(data.get("_processing", False)),
                _completed=bool(data.get("_completed", False)),
            )
        except Exception as exc:
            warnings.warn(f"S3 CacheEntry construction failed: {exc}", stacklevel=2)
            return None

    def _dump_entry(self, entry: CacheEntry) -> bytes:
        """Serialize a CacheEntry to bytes for S3 storage."""
        data = {
            "value": entry.value,
            "time": entry.time.isoformat(),
            "stale": entry.stale,
            "_processing": entry._processing,
            "_completed": entry._completed,
        }
        return pickle.dumps(data)

    # ------------------------------------------------------------------
    # Core interface
    # ------------------------------------------------------------------

    def get_entry_by_key(self, key: str) -> Tuple[str, Optional[CacheEntry]]:
        """Get a cache entry from S3 by its key.

        Parameters
        ----------
        key : str
            The cache key string.

        Returns
        -------
        tuple
            A ``(key, CacheEntry)`` pair, or ``(key, None)`` if not found.

        """
        s3_key = self._get_s3_key(key)
        client = self._get_s3_client()
        try:
            response = client.get_object(Bucket=self.s3_bucket, Key=s3_key)
            body = response["Body"].read()
            entry = self._load_entry(body)
            return key, entry
        except botocore.exceptions.ClientError as exc:
            if exc.response["Error"]["Code"] in ("NoSuchKey", "404"):
                return key, None
            warnings.warn(f"S3 get_entry_by_key failed: {exc}", stacklevel=2)
            return key, None
        except Exception as exc:
            warnings.warn(f"S3 get_entry_by_key failed: {exc}", stacklevel=2)
            return key, None

    def set_entry(self, key: str, func_res: Any) -> bool:
        """Store a function result in S3 under the given key.

        Parameters
        ----------
        key : str
            The cache key string.
        func_res : any
            The function result to cache.

        Returns
        -------
        bool
            ``True`` if the entry was stored, ``False`` otherwise.

        """
        if not self._should_store(func_res):
            return False
        s3_key = self._get_s3_key(key)
        client = self._get_s3_client()
        entry = CacheEntry(
            value=func_res,
            time=datetime.now(),
            stale=False,
            _processing=False,
            _completed=True,
        )
        try:
            client.put_object(Bucket=self.s3_bucket, Key=s3_key, Body=self._dump_entry(entry))
            return True
        except Exception as exc:
            warnings.warn(f"S3 set_entry failed: {exc}", stacklevel=2)
            return False

    def mark_entry_being_calculated(self, key: str) -> None:
        """Mark the given cache entry as currently being calculated.

        Parameters
        ----------
        key : str
            The cache key string.

        """
        s3_key = self._get_s3_key(key)
        client = self._get_s3_client()
        entry = CacheEntry(
            value=None,
            time=datetime.now(),
            stale=False,
            _processing=True,
            _completed=False,
        )
        try:
            client.put_object(Bucket=self.s3_bucket, Key=s3_key, Body=self._dump_entry(entry))
        except Exception as exc:
            warnings.warn(f"S3 mark_entry_being_calculated failed: {exc}", stacklevel=2)

    def mark_entry_not_calculated(self, key: str) -> None:
        """Mark the given cache entry as no longer being calculated.

        Parameters
        ----------
        key : str
            The cache key string.

        """
        s3_key = self._get_s3_key(key)
        client = self._get_s3_client()
        try:
            response = client.get_object(Bucket=self.s3_bucket, Key=s3_key)
            body = response["Body"].read()
            entry = self._load_entry(body)
            if entry is not None:
                entry._processing = False
                client.put_object(Bucket=self.s3_bucket, Key=s3_key, Body=self._dump_entry(entry))
        except botocore.exceptions.ClientError as exc:
            if exc.response["Error"]["Code"] not in ("NoSuchKey", "404"):
                warnings.warn(f"S3 mark_entry_not_calculated failed: {exc}", stacklevel=2)
        except Exception as exc:
            warnings.warn(f"S3 mark_entry_not_calculated failed: {exc}", stacklevel=2)

    def wait_on_entry_calc(self, key: str) -> Any:
        """Poll S3 until the entry is no longer being calculated, then return its value.

        Parameters
        ----------
        key : str
            The cache key string.

        Returns
        -------
        any
            The cached value once calculation is complete.

        """
        time_spent = 0
        while True:
            time.sleep(S3_SLEEP_DURATION_IN_SEC)
            time_spent += S3_SLEEP_DURATION_IN_SEC
            _, entry = self.get_entry_by_key(key)
            if entry is None:
                raise RecalculationNeeded()
            if not entry._processing:
                return entry.value
            self.check_calc_timeout(time_spent)

    def clear_cache(self) -> None:
        """Delete all cache entries for this function from S3."""
        client = self._get_s3_client()
        prefix = self._get_s3_prefix()
        try:
            paginator = client.get_paginator("list_objects_v2")
            pages = paginator.paginate(Bucket=self.s3_bucket, Prefix=prefix)
            objects_to_delete = []
            for page in pages:
                for obj in page.get("Contents", []):
                    objects_to_delete.append({"Key": obj["Key"]})
            if objects_to_delete:
                # S3 delete_objects accepts up to 1000 keys per request
                for i in range(0, len(objects_to_delete), 1000):
                    client.delete_objects(
                        Bucket=self.s3_bucket,
                        Delete={"Objects": objects_to_delete[i : i + 1000]},
                    )
        except Exception as exc:
            warnings.warn(f"S3 clear_cache failed: {exc}", stacklevel=2)

    def clear_being_calculated(self) -> None:
        """Reset the ``_processing`` flag on all entries for this function in S3."""
        client = self._get_s3_client()
        prefix = self._get_s3_prefix()
        try:
            paginator = client.get_paginator("list_objects_v2")
            pages = paginator.paginate(Bucket=self.s3_bucket, Prefix=prefix)
            for page in pages:
                for obj in page.get("Contents", []):
                    s3_key = obj["Key"]
                    try:
                        response = client.get_object(Bucket=self.s3_bucket, Key=s3_key)
                        body = response["Body"].read()
                        entry = self._load_entry(body)
                        if entry is not None and entry._processing:
                            entry._processing = False
                            client.put_object(Bucket=self.s3_bucket, Key=s3_key, Body=self._dump_entry(entry))
                    except Exception as exc:
                        warnings.warn(f"S3 clear_being_calculated entry update failed: {exc}", stacklevel=2)
        except Exception as exc:
            warnings.warn(f"S3 clear_being_calculated failed: {exc}", stacklevel=2)

    def delete_stale_entries(self, stale_after: timedelta) -> None:
        """Remove cache entries older than ``stale_after`` from S3.

        Parameters
        ----------
        stale_after : datetime.timedelta
            Entries older than this duration will be deleted.

        """
        client = self._get_s3_client()
        prefix = self._get_s3_prefix()
        threshold = datetime.now() - stale_after
        try:
            paginator = client.get_paginator("list_objects_v2")
            pages = paginator.paginate(Bucket=self.s3_bucket, Prefix=prefix)
            for page in pages:
                for obj in page.get("Contents", []):
                    s3_key = obj["Key"]
                    try:
                        response = client.get_object(Bucket=self.s3_bucket, Key=s3_key)
                        body = response["Body"].read()
                        entry = self._load_entry(body)
                        if entry is not None and entry.time < threshold:
                            client.delete_object(Bucket=self.s3_bucket, Key=s3_key)
                    except Exception as exc:
                        warnings.warn(f"S3 delete_stale_entries entry check failed: {exc}", stacklevel=2)
        except Exception as exc:
            warnings.warn(f"S3 delete_stale_entries failed: {exc}", stacklevel=2)

    # ------------------------------------------------------------------
    # Async variants delegate to the thread-based defaults in _BaseCore
    # since boto3 is a sync library.
    # ------------------------------------------------------------------
