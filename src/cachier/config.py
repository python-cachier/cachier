import hashlib
import os
import pickle
import threading
from dataclasses import dataclass, field, replace
from datetime import datetime, timedelta
from typing import Any, Optional, Union

from ._types import Backend, HashFunc, Mongetter


def _default_hash_func(args, kwds):
    # Sort the kwargs to ensure consistent ordering
    sorted_kwargs = sorted(kwds.items())
    # Serialize args and sorted_kwargs using pickle or similar
    serialized = pickle.dumps((args, sorted_kwargs))
    # Create a hash of the serialized data
    return hashlib.sha256(serialized).hexdigest()


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


def _update_with_defaults(
    param, name: str, func_kwargs: Optional[dict] = None
):
    import cachier

    if func_kwargs:
        kw_name = f"cachier__{name}"
        if kw_name in func_kwargs:
            return func_kwargs.pop(kw_name)
    if param is None:
        return getattr(cachier.config._global_params, name)
    return param


def set_default_params(**params: Any) -> None:
    """Configure default parameters applicable to all memoized functions."""
    # It is kept for backwards compatibility with desperation warning
    import warnings

    warnings.warn(
        "Called `set_default_params` is deprecated and will be removed."
        " Please use `set_global_params` instead.",
        DeprecationWarning,
        stacklevel=2,
    )
    set_global_params(**params)


def set_global_params(**params: Any) -> None:
    """Configure global parameters applicable to all memoized functions.

    This function takes the same keyword parameters as the ones defined in the
    decorator, which can be passed all at once or with multiple calls.
    Parameters given directly to a decorator take precedence over any values
    set by this function.

    Only 'stale_after', 'next_time', and 'wait_for_calc_timeout' can be changed
    after the memoization decorator has been applied. Other parameters will
    only have an effect on decorators applied after this function is run.

    """
    import cachier

    valid_params = {
        k: v
        for k, v in params.items()
        if hasattr(cachier.config._global_params, k)
    }
    cachier.config._global_params = replace(
        cachier.config._global_params,
        **valid_params,  # type: ignore[arg-type]
    )


def get_default_params() -> Params:
    """Get current set of default parameters."""
    # It is kept for backwards compatibility with desperation warning
    import warnings

    warnings.warn(
        "Called `get_default_params` is deprecated and will be removed."
        " Please use `get_global_params` instead.",
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
