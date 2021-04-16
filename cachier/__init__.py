from .core import cachier, Info
from ._version import get_versions
__version__ = get_versions()['version']
del get_versions
