"""A MongoDB-based caching core for cachier."""

# This file is part of Cachier.
# https://github.com/shaypal5/cachier

# Licensed under the MIT license:
# http://www.opensource.org/licenses/MIT-license
# Copyright (c) 2016, Shay Palachy <shaypal5@gmail.com>

import sys  # to make sure that pymongo was imported
import pickle  # for serialization of python objects
from datetime import datetime
import time   # to sleep when waiting on Mongo cache\
import warnings  # to warn if pymongo is missing

try:
    from pymongo import (
        IndexModel,
        ASCENDING
    )
    from pymongo.errors import OperationFailure
    from bson.binary import Binary  # to save binary data to mongodb
except ImportError:  # pragma: no cover
    pass

from .base_core import _BaseCore


MONGO_SLEEP_DURATION_IN_SEC = 1


class RecalculationNeeded(Exception):
    pass


class _MongoCore(_BaseCore):

    _INDEX_NAME = 'func_1_key_1'

    def __init__(self, mongetter, stale_after, next_time):
        if 'pymongo' not in sys.modules:
            warnings.warn((
                "Cachier warning: pymongo was not found. "
                "MongoDB cores will not function."))
        _BaseCore.__init__(self, stale_after, next_time)
        self.mongetter = mongetter
        self.mongo_collection = self.mongetter()
        index_inf = self.mongo_collection.index_information()
        if _MongoCore._INDEX_NAME not in index_inf:
            func1key1 = IndexModel(
                keys=[('func', ASCENDING), ('key', ASCENDING)],
                name=_MongoCore._INDEX_NAME)
            self.mongo_collection.create_indexes([func1key1])

    @staticmethod
    def _get_func_str(func):
        return '.{}.{}'.format(func.__module__, func.__name__)

    def get_entry_by_key(self, key):
        res = self.mongo_collection.find_one({
            'func': _MongoCore._get_func_str(self.func),
            'key': key
        })
        if res:
            try:
                entry = {
                    'value': pickle.loads(res['value']),
                    'time': res.get('time', None),
                    'stale': res.get('stale', False),
                    'being_calculated': res.get('being_calculated', False)
                }
            except KeyError:
                entry = {
                    'value': None,
                    'time': res.get('time', None),
                    'stale': res.get('stale', False),
                    'being_calculated': res.get('being_calculated', False)
                }
            return key, entry
        return key, None

    def get_entry(self, args, kwds, hash_params):
        key = pickle.dumps(args + tuple(sorted(kwds.items())) if hash_params is None else hash_params(args, kwds))
        return self.get_entry_by_key(key)

    def set_entry(self, key, func_res):
        thebytes = pickle.dumps(func_res)
        self.mongo_collection.update_one(
            filter={
                'func': _MongoCore._get_func_str(self.func),
                'key': key
            },
            update={
                '$set': {
                    'func': _MongoCore._get_func_str(self.func),
                    'key': key,
                    'value': Binary(thebytes),
                    'time': datetime.now(),
                    'stale': False,
                    'being_calculated': False
                }
            },
            upsert=True
        )

    def mark_entry_being_calculated(self, key):
        self.mongo_collection.update_one(
            filter={
                'func': _MongoCore._get_func_str(self.func),
                'key': key
            },
            update={
                '$set': {'being_calculated': True}
            },
            upsert=True
        )

    def mark_entry_not_calculated(self, key):
        try:
            self.mongo_collection.update_one(
                filter={
                    'func': _MongoCore._get_func_str(self.func),
                    'key': key
                },
                update={
                    '$set': {'being_calculated': False}
                },
                upsert=False  # should not insert in this case
            )
        except OperationFailure:
            pass  # don't care in this case

    def wait_on_entry_calc(self, key):
        while True:
            time.sleep(MONGO_SLEEP_DURATION_IN_SEC)
            key, entry = self.get_entry_by_key(key)
            if entry is None:
                raise RecalculationNeeded()
            if entry is not None and not entry['being_calculated']:
                return entry['value']

    def clear_cache(self):
        self.mongo_collection.delete_many(
            filter={'func': _MongoCore._get_func_str(self.func)}
        )

    def clear_being_calculated(self):
        self.mongo_collection.update_many(
            filter={
                'func': _MongoCore._get_func_str(self.func),
                'being_calculated': True
            },
            update={
                '$set': {'being_calculated': False}
            }
        )
