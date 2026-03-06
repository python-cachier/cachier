from typing import TYPE_CHECKING, Any, Awaitable, Callable, Literal, Union

if TYPE_CHECKING:
    import pymongo.collection
    import redis


HashFunc = Callable[..., str]
Mongetter = Callable[[], Union["pymongo.collection.Collection", Awaitable["pymongo.collection.Collection"]]]
RedisClient = Union["redis.Redis", Callable[[], Union["redis.Redis", Awaitable["redis.Redis"]]]]
S3Client = Union[Any, Callable[[], Any]]
Backend = Literal["pickle", "mongo", "memory", "redis", "s3"]
