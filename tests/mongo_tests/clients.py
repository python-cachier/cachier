"""Mongo test client implementations shared across Mongo test modules."""


class _AsyncInMemoryMongoCollection:
    """Minimal in-memory Mongo-like collection with async methods only."""

    def __init__(self):
        self._docs: dict[tuple[str, str], dict[str, object]] = {}
        self._indexes: dict[str, dict[str, object]] = {}

    async def index_information(self):
        return dict(self._indexes)

    async def create_indexes(self, indexes):
        for index in indexes:
            document = getattr(index, "document", {})
            name = document.get("name", "index") if isinstance(document, dict) else "index"
            self._indexes[name] = {"name": name}
        return list(self._indexes)

    async def find_one(self, query=None, **kwargs):
        if query is None:
            query = kwargs.get("filter", {})
        doc = self._docs.get((query.get("func"), query.get("key")))
        return None if doc is None else dict(doc)

    async def update_one(self, query=None, update=None, upsert=False, **kwargs):
        if query is None:
            query = kwargs.get("filter", {})
        if update is None:
            update = kwargs.get("update", {})
        key = (query.get("func"), query.get("key"))
        doc = self._docs.get(key)
        if doc is None:
            if not upsert:
                return {"matched_count": 0}
            doc = {"func": query.get("func"), "key": query.get("key")}
        doc.update(update.get("$set", {}))
        self._docs[key] = doc
        return {"matched_count": 1}

    async def update_many(self, query=None, update=None, **kwargs):
        if query is None:
            query = kwargs.get("filter", {})
        if update is None:
            update = kwargs.get("update", {})
        changed = 0
        for doc in self._docs.values():
            if "func" in query and doc.get("func") != query["func"]:
                continue
            if "processing" in query and doc.get("processing") != query["processing"]:
                continue
            doc.update(update.get("$set", {}))
            changed += 1
        return {"matched_count": changed}

    async def delete_many(self, query=None, **kwargs):
        if query is None:
            query = kwargs.get("filter", {})
        deleted = 0
        time_filter = query.get("time")
        for key, doc in list(self._docs.items()):
            if "func" in query and doc.get("func") != query["func"]:
                continue
            if isinstance(time_filter, dict) and "$lt" in time_filter:
                doc_time = doc.get("time")
                if doc_time is None or doc_time >= time_filter["$lt"]:
                    continue
            del self._docs[key]
            deleted += 1
        return {"deleted_count": deleted}
