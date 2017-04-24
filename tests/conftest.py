
import pytest

from .test_mongo_core import _test_mongetter


def mongo_finalizer():
    """The finalizer for MongoCore-related tests."""
    print('\n Tearing down MongoCore-related assets...')
    collection = _test_mongetter()
    collection.drop_indexes()
    print(" - Indexes dropped from cachier collection.")


@pytest.fixture(scope="session", autouse=True)
def do_something(request):
    """Sessions-scopre pytest hook."""
    request.addfinalizer(mongo_finalizer)
