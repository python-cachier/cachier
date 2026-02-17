"""Redis test client implementations shared across Redis test modules."""

from fnmatch import fnmatch


class _MockPipeline:
    """Minimal pipeline mock used by sync Redis mock client."""

    def __init__(self, redis_client):
        self.redis_client = redis_client
        self.commands = []

    def hset(self, key, field, value):
        self.commands.append(("hset", key, field, value))
        return self

    def execute(self):
        for cmd, key, field, value in self.commands:
            if cmd == "hset":
                self.redis_client.hset(key, field=field, value=value)


class _MockRedis:
    """Sync in-memory Redis-like client for tests."""

    def __init__(self):
        self.data = {}
        print("DEBUG: MockRedis initialized")

    def hgetall(self, key):
        result = self.data.get(key, {})
        bytes_result = {}
        for k, v in result.items():
            if isinstance(v, str):
                bytes_result[k.encode("utf-8")] = v.encode("utf-8")
            else:
                bytes_result[k.encode("utf-8")] = v
        print(f"DEBUG: hgetall({key}) = {result} -> {bytes_result}")
        return bytes_result

    def hset(self, key, field=None, value=None, mapping=None, **kwargs):
        if key not in self.data:
            self.data[key] = {}
        if mapping is not None:
            self.data[key].update(mapping)
        elif field is not None and value is not None:
            self.data[key][field] = value
        elif kwargs:
            self.data[key].update(kwargs)
        print(
            f"DEBUG: hset({key}, field={field}, value={value}, mapping={mapping}, kwargs={kwargs}) -> {self.data[key]}"
        )

    def keys(self, pattern):
        import re

        pattern = pattern.replace("*", ".*")
        result = [k for k in self.data if re.match(pattern, k)]
        print(f"DEBUG: keys({pattern}) = {result}")
        return result

    def delete(self, *keys):
        for key in keys:
            self.data.pop(key, None)
        print(f"DEBUG: delete({keys})")

    def pipeline(self):
        return _MockPipeline(self)

    def ping(self):
        return True

    def set(self, key, value):
        self.data[key] = value
        print(f"DEBUG: set({key}, {value})")

    def get(self, key):
        result = self.data.get(key)
        if isinstance(result, str):
            result = result.encode("utf-8")
        print(f"DEBUG: get({key}) = {result}")
        return result


class _SyncInMemoryRedis:
    """Minimal sync Redis-like client exposing required hash operations."""

    def hgetall(self, key: str) -> dict[bytes, object]:
        return {}

    def hset(self, key: str, field=None, value=None, mapping=None, **kwargs):
        return None

    def keys(self, pattern: str) -> list[str]:
        return []

    def delete(self, *keys: str):
        return None

    def hget(self, key: str, field: str):
        return None


class _AsyncInMemoryRedis:
    """Minimal async Redis-like client implementing required hash operations."""

    def __init__(self):
        self._data: dict[str, dict[str, object]] = {}
        self.fail_hgetall = False
        self.fail_hset = False
        self.fail_keys = False
        self.fail_delete = False
        self.fail_hget = False

    async def hgetall(self, key: str) -> dict[bytes, object]:
        if self.fail_hgetall:
            raise Exception("hgetall failed")
        raw = self._data.get(key, {})
        res: dict[bytes, object] = {}
        for k, v in raw.items():
            res[k.encode("utf-8")] = v.encode("utf-8") if isinstance(v, str) else v
        return res

    async def hset(self, key: str, field=None, value=None, mapping=None, **kwargs):
        if self.fail_hset:
            raise Exception("hset failed")
        if key not in self._data:
            self._data[key] = {}
        if mapping is not None:
            self._data[key].update(mapping)
            return
        if field is not None and value is not None:
            self._data[key][field] = value
            return
        if kwargs:
            self._data[key].update(kwargs)

    async def keys(self, pattern: str) -> list[str]:
        if self.fail_keys:
            raise Exception("keys failed")
        return [key for key in self._data if fnmatch(key, pattern)]

    async def delete(self, *keys: str):
        if self.fail_delete:
            raise Exception("delete failed")
        for key in keys:
            self._data.pop(key, None)

    async def hget(self, key: str, field: str):
        if self.fail_hget:
            raise Exception("hget failed")
        return self._data.get(key, {}).get(field)


class _PartialAsyncRedis:
    """Invalid sync/async Redis client with only some async methods."""

    async def hgetall(self, key):
        return {}

    async def hset(self, key, **kwargs):
        pass

    def keys(self, pattern):
        return []

    def delete(self, *keys):
        pass

    def hget(self, key, field):
        return None


class _NoMethodsObject:
    """Plain object with no Redis methods at all."""

    pass
