"""Configuration file for pytest."""

import pytest
# import shutil

from .test_mongo_core import _test_mongetter
# from cachier.pickle_core import EXPANDED_CACHIER_DIR


def mongo_finalizer():
    """The finalizer for MongoCore-related tests."""
    print('\n Tearing down MongoCore-related assets...')
    collection = _test_mongetter()
    collection.drop_indexes()
    print(" - Indexes dropped from cachier collection.")


@pytest.fixture(scope="session", autouse=True)
def do_something(request):
    """Session-scope pytest hook."""
    # shutil.rmtree(EXPANDED_CACHIER_DIR)
    request.addfinalizer(mongo_finalizer)
