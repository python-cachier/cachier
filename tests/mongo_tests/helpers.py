"""Mongo test helpers: client configuration and shared mongetter."""

import platform
import sys
from contextlib import suppress
from urllib.parse import quote_plus

from birch import Birch  # type: ignore[import-not-found]

try:
    from pymongo.mongo_client import MongoClient
except (ImportError, ModuleNotFoundError):

    class MongoClient:
        """Mock MongoClient raising ImportError when pymongo is missing."""

        def __init__(self, *args, **kwargs):
            """Initialize."""
            raise ImportError("pymongo is not installed!")


try:
    from pymongo_inmemory import MongoClient as InMemoryMongoClient
except (ImportError, ModuleNotFoundError):

    class InMemoryMongoClient:
        """Mock InMemoryMongoClient raising ImportError when pymongo_inmemory is missing."""

        def __init__(self, *args, **kwargs):
            """Initialize."""
            raise ImportError("pymongo_inmemory is not installed!")


class CfgKey:
    HOST = "TEST_HOST"
    PORT = "TEST_PORT"
    TEST_VS_DOCKERIZED_MONGO = "TEST_VS_DOCKERIZED_MONGO"


CFG = Birch(namespace="cachier", defaults={CfgKey.TEST_VS_DOCKERIZED_MONGO: False})
_COLLECTION_NAME = f"cachier_test_{platform.system()}_{'.'.join(map(str, sys.version_info[:3]))}"


def _get_cachier_db_mongo_client():
    host = quote_plus(CFG[CfgKey.HOST])
    port = quote_plus(CFG[CfgKey.PORT])
    uri = f"mongodb://{host}:{port}?retrywrites=true&w=majority"
    return MongoClient(uri)


def _test_mongetter():
    if not hasattr(_test_mongetter, "client"):
        if str(CFG.mget(CfgKey.TEST_VS_DOCKERIZED_MONGO)).lower() == "true":
            print("Using live MongoDB instance for testing.")
            _test_mongetter.client = _get_cachier_db_mongo_client()
        else:
            print("Using in-memory MongoDB instance for testing.")
            _test_mongetter.client = InMemoryMongoClient()
    db_obj = _test_mongetter.client["cachier_test"]
    if _COLLECTION_NAME not in db_obj.list_collection_names():
        db_obj.create_collection(_COLLECTION_NAME)
    return db_obj[_COLLECTION_NAME]


def _cleanup_mongo_client():
    """Close any cached Mongo test client safely."""
    client = getattr(_test_mongetter, "client", None)
    if client is None:
        return
    with suppress(Exception):
        client._mongod._client.close()  # type: ignore[attr-defined]
    with suppress(Exception):
        client.close()
    with suppress(Exception):
        delattr(_test_mongetter, "client")
