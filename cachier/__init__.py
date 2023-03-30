from .core import cachier, set_default_params, get_default_params
from ._version import get_versions
__version__ = get_versions()['version']
del get_versions
# flake8: noqa
