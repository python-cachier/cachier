from typing import TYPE_CHECKING, Callable, Literal

if TYPE_CHECKING:
    import pymongo.collection


HashFunc = Callable[..., str]
Mongetter = Callable[[], "pymongo.collection.Collection"]
Backend = Literal["pickle", "mongo", "memory"]
