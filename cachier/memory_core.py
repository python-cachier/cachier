"""A memory-based caching core for cachier."""

from collections import defaultdict
from datetime import datetime

from .base_core import _BaseCore


class _MemoryCore(_BaseCore):
    """The pickle core class for cachier.

    Parameters
    ----------
    stale_after : datetime.timedelta, optional
        See :class:`_BaseCore` documentation.
    next_time : bool, optional
        See :class:`_BaseCore` documentation.
    """

    def __init__(self, stale_after, next_time):
        super().__init__(stale_after=stale_after, next_time=next_time)
        self.cache = defaultdict(dict)

    def get_entry_by_key(self, key, reload=False):  # pylint: disable=W0221
        return key, self.cache.get(key, None)

    def get_entry(self, args, kwds, hash_params):
        key = args + tuple(sorted(kwds.items())) if hash_params is None else hash_params(args, kwds)
        return self.get_entry_by_key(key)

    def set_entry(self, key, func_res):
        self.cache[key] = {
            'value': func_res,
            'time': datetime.now(),
            'stale': False,
            'being_calculated': False,
        }

    def mark_entry_being_calculated(self, key):
        try:
            self.cache[key]['being_calculated'] = True
        except KeyError:
            self.cache[key] = {
                'value': None,
                'time': datetime.now(),
                'stale': False,
                'being_calculated': True,
            }

    def mark_entry_not_calculated(self, key):
        try:
            self.cache[key]['being_calculated'] = False
        except KeyError:
            pass  # that's ok, we don't need an entry in that case

    def wait_on_entry_calc(self, key):
        entry = self.cache[key]
        # I don't think waiting is necessary for this one
        # if not entry['being_calculated']:
        return entry['value']

    def clear_cache(self):
        self.cache.clear()

    def clear_being_calculated(self):
        for value in self.cache.values():
            value['being_calculated'] = False
