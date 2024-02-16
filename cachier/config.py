import datetime
import hashlib
import os
import pickle
from typing import Callable, Optional, Union, TypedDict, TYPE_CHECKING
from typing_extensions import Literal

if TYPE_CHECKING:
    import pymongo.collection


_Type_HashFunc = Callable[..., str]
_Type_Mongetter = Callable[[], "pymongo.collection.Collection"]
_Type_Backend = Literal["pickle", "mongo", "memory"]


def _default_hash_func(args, kwds):
    # Sort the kwargs to ensure consistent ordering
    sorted_kwargs = sorted(kwds.items())
    # Serialize args and sorted_kwargs using pickle or similar
    serialized = pickle.dumps((args, sorted_kwargs))
    # Create a hash of the serialized data
    return hashlib.sha256(serialized).hexdigest()


class Params(TypedDict):
    caching_enabled: bool
    hash_func: _Type_HashFunc
    backend: _Type_Backend
    mongetter: Optional[_Type_Mongetter]
    stale_after: datetime.timedelta
    next_time: bool
    cache_dir: Union[str, os.PathLike]
    pickle_reload: bool
    separate_files: bool
    wait_for_calc_timeout: int
    allow_none: bool


_default_params: Params = {
    "caching_enabled": True,
    "hash_func": _default_hash_func,
    "backend": "pickle",
    "mongetter": None,
    "stale_after": datetime.timedelta.max,
    "next_time": False,
    "cache_dir": "~/.cachier/",
    "pickle_reload": True,
    "separate_files": False,
    "wait_for_calc_timeout": 0,
    "allow_none": False,
}


def _update_with_defaults(param, name: str):
    if param is None:
        return _default_params[name]
    return param


def set_default_params(**params):
    """Configure global parameters applicable to all memoized functions.

    This function takes the same keyword parameters as the ones defined in the
    decorator, which can be passed all at once or with multiple calls.
    Parameters given directly to a decorator take precedence over any values
    set by this function.

    Only 'stale_after', 'next_time', and 'wait_for_calc_timeout' can be changed
    after the memoization decorator has been applied. Other parameters will
    only have an effect on decorators applied after this function is run.

    """
    valid_params = (p for p in params.items() if p[0] in _default_params)
    _default_params.update(valid_params)


def get_default_params():
    """Get current set of default parameters."""
    return _default_params


def enable_caching():
    """Enable caching globally."""
    _default_params["caching_enabled"] = True


def disable_caching():
    """Disable caching globally."""
    _default_params["caching_enabled"] = False
