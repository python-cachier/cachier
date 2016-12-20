"""A MongoDB-based caching core for cachier."""

# This file is part of Cachier.
# https://github.com/shaypal5/cachier

# Licensed under the MIT license:
# http://www.opensource.org/licenses/MIT-license
# Copyright (c) 2016, Shay Palachy <shaypal5@gmail.com>

import pickle  # for serialization of python objects
from datetime import datetime
import time   # to sleep when waiting on Mongo cache

from pymongo import (
    IndexModel,
    ASCENDING
)
from pymongo.errors import OperationFailure
from bson.binary import Binary  # to save binary data to mongodb

from .base_core import _BaseCore


MONGO_SLEEP_DURATION_IN_SEC = 6


class _MongoCore(_BaseCore):

    def __init__(self, mongetter, stale_after, next_time):
        _BaseCore.__init__(self, stale_after, next_time)
        self.mongetter = mongetter
        self.mongo_collection = self.mongetter()
        if '_func_1_key_1' not in self.mongo_collection.index_information():
            func1key1 = IndexModel(
                [('func', ASCENDING), ('key', ASCENDING)],
                name='_func_1_key_1')
            self.mongo_collection.create_indexes([func1key1])

    @staticmethod
    def _get_func_str(func):
        return '.{}.{}'.format(func.__module__, func.__name__)

    def _get_mongo_collection(self):
        if not self.mongo_collection:
            self.mongo_collection = self.mongetter()
        return self.mongo_collection

    def get_entry_by_key(self, key):
        res = self._get_mongo_collection().find_one({
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

    def get_entry(self, args, kwds):
        key = pickle.dumps(args + tuple(sorted(kwds.items())))
        # print('key type={}, key={}'.format(
        #     type(key), key))
        return self.get_entry_by_key(key)

    def set_entry(self, key, func_res):
        thebytes = pickle.dumps(func_res)
        self._get_mongo_collection().update_one(
            {
                'func': _MongoCore._get_func_str(self.func),
                'key': key
            },
            {
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
        self._get_mongo_collection().update_one(
            {
                'func': _MongoCore._get_func_str(self.func),
                'key': key
            },
            {
                '$set': {'being_calculated': True}
            },
            upsert=True
        )

    def mark_entry_not_calculated(self, key):
        try:
            self._get_mongo_collection().update_one(
                {
                    'func': _MongoCore._get_func_str(self.func),
                    'key': key
                },
                {
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
            if entry is not None and not entry['being_calculated']:
                return entry['value']
        # key, entry = self.get_entry_by_key(key)
        # if entry is not None:
        #     return entry['value']
        # return None

    def clear_cache(self):
        self._get_mongo_collection().delete_many(
            {'func': _MongoCore._get_func_str(self.func)})

    def clear_being_calculated(self):
        self._get_mongo_collection().update_many(
            {
                'func': _MongoCore._get_func_str(self.func),
                'being_calculated': True
            },
            {
                '$set': {'being_calculated': False}
            }
        )
