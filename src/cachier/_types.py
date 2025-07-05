from typing import TYPE_CHECKING, Callable, Literal, Union

if TYPE_CHECKING:
    import pymongo.collection
    import redis


HashFunc = Callable[..., str]
Mongetter = Callable[[], "pymongo.collection.Collection"]
RedisClient = Union["redis.Redis", Callable[[], "redis.Redis"]]
Backend = Literal["pickle", "mongo", "memory", "redis"]
