import hashlib
import os
import pickle
import threading
from dataclasses import dataclass, field, replace
from datetime import datetime, timedelta
from typing import Any, Optional, Union

from ._types import Backend, HashFunc, Mongetter


def _is_numpy_array(value: Any) -> bool:
    """Check whether a value is a NumPy ndarray without importing NumPy eagerly.

    Parameters
    ----------
    value : Any
        The value to inspect.

    Returns
    -------
    bool
        True when ``value`` is a NumPy ndarray instance.

    """
    return type(value).__module__ == "numpy" and type(value).__name__ == "ndarray"


def _hash_numpy_array(hasher: "hashlib._Hash", value: Any) -> None:
    """Update hasher with NumPy array metadata and buffer content.

    Parameters
    ----------
    hasher : hashlib._Hash
        The hasher to update.
    value : Any
        A NumPy ndarray instance.

    """
    hasher.update(b"numpy.ndarray")
    hasher.update(value.dtype.str.encode("utf-8"))
    hasher.update(str(value.shape).encode("utf-8"))
    hasher.update(value.tobytes(order="C"))


def _update_hash_for_value(hasher: "hashlib._Hash", value: Any) -> None:
    """Update hasher with a stable representation of a Python value.

    Parameters
    ----------
    hasher : hashlib._Hash
        The hasher to update.
    value : Any
        Value to encode.

    """
    if _is_numpy_array(value):
        _hash_numpy_array(hasher, value)
        return

    if isinstance(value, tuple):
        hasher.update(b"tuple")
        for item in value:
            _update_hash_for_value(hasher, item)
        return

    if isinstance(value, list):
        hasher.update(b"list")
        for item in value:
            _update_hash_for_value(hasher, item)
        return

    if isinstance(value, dict):
        hasher.update(b"dict")
        for dict_key in sorted(value):
            _update_hash_for_value(hasher, dict_key)
            _update_hash_for_value(hasher, value[dict_key])
        return

    if isinstance(value, (set, frozenset)):
        # Use a deterministic ordering of elements for hashing.
        hasher.update(b"frozenset" if isinstance(value, frozenset) else b"set")
        try:
            # Fast path: works for homogeneous, orderable element types.
            iterable = sorted(value)
        except TypeError:
            # Fallback: impose a deterministic order based on type name and repr.
            iterable = sorted(value, key=lambda item: (type(item).__name__, repr(item)))
        for item in iterable:
            _update_hash_for_value(hasher, item)
        return
    hasher.update(pickle.dumps(value, protocol=pickle.HIGHEST_PROTOCOL))


def _default_hash_func(args, kwds):
    """Compute a stable hash key for function arguments.

    Parameters
    ----------
    args : tuple
        Positional arguments.
    kwds : dict
        Keyword arguments.

    Returns
    -------
    str
        A hex digest representing the call arguments.

    """
    hasher = hashlib.blake2b(digest_size=32)
    hasher.update(b"args")
    _update_hash_for_value(hasher, args)
    hasher.update(b"kwds")
    _update_hash_for_value(hasher, dict(sorted(kwds.items())))
    return hasher.hexdigest()


def _default_cache_dir():
    """Return default cache directory based on XDG specification.

    Uses $XDG_CACHE_HOME if defined, otherwise falls back to ~/.cachier/

    """
    xdg_cache_home = os.environ.get("XDG_CACHE_HOME")
    if xdg_cache_home:
        # Use XDG-compliant cache directory
        return os.path.join(xdg_cache_home, "cachier")
    # Default fallback if XDG is not set
    return os.path.expanduser("~/.cachier/")


class LazyCacheDir:
    """Lazily resolve the default cache directory using $XDG_CACHE_HOME."""

    def __str__(self):
        """Return the resolved cache directory path as a string."""
        return _default_cache_dir()

    def __fspath__(self):
        """Return the path for filesystem operations."""
        return self.__str__()

    def __eq__(self, other):
        """Compare the resolved path to another path."""
        return str(self) == str(other)


@dataclass
class Params:
    """Default definition for cachier parameters."""

    caching_enabled: bool = True
    hash_func: HashFunc = _default_hash_func
    backend: Backend = "pickle"
    mongetter: Optional[Mongetter] = None
    stale_after: timedelta = timedelta.max
    next_time: bool = False
    cache_dir: Union[str, os.PathLike] = field(default_factory=LazyCacheDir)
    pickle_reload: bool = True
    separate_files: bool = False
    wait_for_calc_timeout: int = 0
    allow_none: bool = False
    cleanup_stale: bool = False
    cleanup_interval: timedelta = timedelta(days=1)
    entry_size_limit: Optional[int] = None


_global_params = Params()


@dataclass
class CacheEntry:
    """Data class for cache entries."""

    value: Any
    time: datetime
    stale: bool
    _processing: bool
    _condition: Optional[threading.Condition] = None
    _completed: bool = False


def _update_with_defaults(param, name: str, func_kwargs: Optional[dict] = None):
    import cachier

    if func_kwargs:
        kw_name = f"cachier__{name}"
        if kw_name in func_kwargs:
            return func_kwargs.pop(kw_name)
    if param is None:
        return getattr(cachier.config._global_params, name)
    return param


def set_default_params(**params: Any) -> None:
    """Configure default parameters applicable to all memoized functions.

    Deprecated, use :func:`~cachier.config.set_global_params` instead.

    """
    # It is kept for backwards compatibility with desperation warning
    import warnings

    warnings.warn(
        "Called `set_default_params` is deprecated and will be removed. Please use `set_global_params` instead.",
        DeprecationWarning,
        stacklevel=2,
    )
    set_global_params(**params)


def set_global_params(**params: Any) -> None:
    """Configure global parameters applicable to all memoized functions.

    This function takes the same keyword parameters as the ones defined in the
    decorator. Parameters given directly to a decorator take precedence over
    any values set by this function.

    Note on dynamic behavior:
    - If a decorator parameter is provided explicitly (not None), that value
      is used for the decorated function and is not affected by later changes
      to the global parameters.
    - If a decorator parameter is left as None, the decorator/core may read
      the corresponding value from the global params at call time. Parameters
      that are read dynamically (when decorator parameter was None) include:
      'stale_after', 'next_time', 'allow_none', 'cleanup_stale',
      'cleanup_interval', and 'caching_enabled'. In some cores, if the
      decorator was created without concrete value for 'wait_for_calc_timeout',
      calls that check calculation timeouts will fall back to the global
      'wait_for_calc_timeout' as well.

    """
    import cachier

    valid_params = {k: v for k, v in params.items() if hasattr(cachier.config._global_params, k)}
    cachier.config._global_params = replace(
        cachier.config._global_params,
        **valid_params,
    )


def get_default_params() -> Params:
    """Get current set of default parameters.

    Deprecated, use :func:`~cachier.config.get_global_params` instead.

    """
    # It is kept for backwards compatibility with desperation warning
    import warnings

    warnings.warn(
        "Called `get_default_params` is deprecated and will be removed. Please use `get_global_params` instead.",
        DeprecationWarning,
        stacklevel=2,
    )
    return get_global_params()


def get_global_params() -> Params:
    """Get current set of default parameters."""
    import cachier

    return cachier.config._global_params


def enable_caching():
    """Enable caching globally."""
    import cachier

    cachier.config._global_params.caching_enabled = True


def disable_caching():
    """Disable caching globally."""
    import cachier

    cachier.config._global_params.caching_enabled = False
