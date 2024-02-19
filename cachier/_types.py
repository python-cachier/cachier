from typing import Callable, TYPE_CHECKING, Literal

if TYPE_CHECKING:
    import pymongo.collection


HashFunc = Callable[..., str]
Mongetter = Callable[[], "pymongo.collection.Collection"]
Backend = Literal["pickle", "mongo", "memory"]
