"""Prepares a pickle cache for the text hashing test function."""


from cachier import cachier
from time import sleep
from random import random
from pickle import dump, dumps
from zlib import adler32


TEXT_VAL_TO_CHECK = 'foo'
TEXT_CACHE_FNAME = 'cachier_text_cache_temp.pkl'


@cachier(pickle_reload=False)
def text_caching(text):
    sleep(1)
    print(text)
    print(adler32(dumps(text)) & 0xffffffff)
    return random()


if __name__ == '__main__':
    text_caching.clear_cache()
    return_val = text_caching(TEXT_VAL_TO_CHECK)
    print(return_val)
    with open(TEXT_CACHE_FNAME, 'wb+') as f:
        dump(return_val, f)
