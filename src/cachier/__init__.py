from ._version import __version__
from .config import (
    disable_caching,
    enable_caching,
    get_default_params,
    get_global_params,
    set_default_params,
    set_global_params,
)
from .core import cachier

__all__ = [
    "cachier",
    "set_default_params",
    "get_default_params",
    "set_global_params",
    "get_global_params",
    "enable_caching",
    "disable_caching",
    "__version__",
]
