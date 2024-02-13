Cachier
#######

|PyPI-Status| |Downloads| |PyPI-Versions| |Build-Status| |Codecov| |Codefactor| |LICENCE|

Persistent, stale-free, local and cross-machine caching for Python functions.

.. code-block:: python

  from cachier import cachier
  import datetime

  @cachier(stale_after=datetime.timedelta(days=3))
  def foo(arg1, arg2):
    """foo now has a persistent cache, triggering recalculation for values stored more than 3 days."""
    return {'arg1': arg1, 'arg2': arg2}


.. role:: python(code)
  :language: python

.. contents::

.. section-numbering:



Installation
============

Install ``cachier`` with:

.. code-block:: python

    pip install cachier

For the latest version supporting Python 2.7 please use:

.. code-block:: python

    pip install 'cachier==1.2.8'

Features
========

* Pure Python.
* Compatible with Python 3.8+ (Python 2.7 was discontinued in version 1.2.8).
* Supported and `tested on Linux, OS X and Windows <https://travis-ci.org/shaypal5/cachier>`_.
* A simple interface.
* Defining "shelf life" for cached values.
* Local caching using pickle files.
* Cross-machine caching using MongoDB.
* Thread-safety.

Cachier is **NOT**:

* Meant as a transient cache. Python's @lru_cache is better.
* Especially fast. It is meant to replace function calls that take more than... a second, say (overhead is around 1 millisecond).

Future features
---------------

* S3 core.
* Multi-core caching.
* `Cache replacement policies <https://en.wikipedia.org/wiki/Cache_replacement_policies>`_


Use
===

Cachier provides a decorator which you can wrap around your functions to give them a persistent cache. The positional and keyword arguments to the wrapped function must be hashable (i.e. Python's immutable built-in objects, not mutable containers). Also, notice that since objects which are instances of user-defined classes are hashable but all compare unequal (their hash value is their id), equal objects across different sessions will not yield identical keys.

Setting up a Cache
------------------
You can add a default, pickle-based, persistent cache to your function - meaning it will last across different Python kernels calling the wrapped function - by decorating it with the ``cachier`` decorator (notice the ``()``!).

.. code-block:: python

  from cachier import cachier

  @cachier()
  def foo(arg1, arg2):
    """Your function now has a persistent cache mapped by argument values!"""
    return {'arg1': arg1, 'arg2': arg2}

Class and object methods can also be cached. Cachier will automatically ignore the `self` parameter when determining the cache key for an object method. **This means that methods will be cached across all instances of an object, which may not be what you want.**

.. code-block:: python

  from cachier import cachier

  class Foo():
    @staticmethod
    @cachier()
    def good_static_usage(arg_1, arg_2):
      return arg_1 + arg_2

    # Instance method does not depend on object's internal state, so good to cache
    @cachier()
    def good_usage_1(self, arg_1, arg_2):
      return arg_1 + arg_2

    # Instance method is calling external service, probably okay to cache
    @cachier()
    def good_usage_2(self, arg_1, arg_2):
      result = self.call_api(arg_1, arg_2)
      return result

    # Instance method relies on object attribute, NOT good to cache
    @cachier()
    def bad_usage(self, arg_1, arg_2):
      return arg_1 + arg_2 + self.arg_3


Resetting a Cache
-----------------
The Cachier wrapper adds a ``clear_cache()`` function to each wrapped function. To reset the cache of the wrapped function simply call this method:

.. code-block:: python

  foo.clear_cache()

General Configuration
----------------------

Global Defaults
~~~~~~~~~~~~~~~

Settings can be globally configured across all Cachier wrappers through the use of the `set_default_params` function. This function takes the same keyword parameters as the ones defined in the decorator, which can be passed all at once or with multiple calls. Parameters given directly to a decorator take precedence over any values set by this function.

The following parameters will only be applied to decorators defined after `set_default_params` is called:

*  `hash_func`
*  `backend`
*  `mongetter`
*  `cache_dir`
*  `pickle_reload`
*  `separate_files`

These parameters can be changed at any time and they will apply to all decorators:

*  `allow_none`
*  `caching_enabled`
*  `stale_after`
*  `next_time`
*  `wait_for_calc_timeout`

The current defaults can be fetched by calling `get_default_params`.

Threads Limit
~~~~~~~~~~~~~

To limit the number of threads Cachier is allowed to spawn, set the ``CACHIER_MAX_WORKERS`` with the desired number. The default is 8, so to enable Cachier to spawn even more threads, you'll have to set a higher limit explicitly.


Global Enable/Disable
---------------------

Caching can be turned off across all decorators by calling `disable_caching`, and then re-activated by calling `enable_caching`.

These functions are convenience wrappers around the `caching_enabled` default setting.


Cache Shelf Life
----------------

Setting Shelf Life
~~~~~~~~~~~~~~~~~~
You can set any duration as the shelf life of cached return values of a function by providing a corresponding ``timedelta`` object to the ``stale_after`` parameter:

.. code-block:: python

  import datetime

  @cachier(stale_after=datetime.timedelta(weeks=2))
  def bar(arg1, arg2):
    return {'arg1': arg1, 'arg2': arg2}

Now when a cached value matching the given arguments is found the time of its calculation is checked; if more than ``stale_after`` time has since passed, the function will be run again for the same arguments and the new value will be cached and returned.

This is useful for lengthy calculations that depend on a dynamic data source.

Fuzzy Shelf Life
~~~~~~~~~~~~~~~~
Sometimes you may want your function to trigger a calculation when it encounters a stale result, but still not wait on it if it's not that critical. In that case, you can set ``next_time`` to ``True`` to have your function trigger a recalculation **in a separate thread**, but return the currently cached stale value:

.. code-block:: python

  @cachier(next_time=True)

Further function calls made while the calculation is being performed will not trigger redundant calculations.


Working with unhashable arguments
---------------------------------

As mentioned above, the positional and keyword arguments to the wrapped function must be hashable (i.e. Python's immutable built-in objects, not mutable containers). To get around this limitation the ``hash_func`` parameter of the ``cachier`` decorator can be provided with a callable that gets the args and kwargs from the decorated function and returns a hash key for them.

.. code-block:: python

  def calculate_hash(args, kwds):
    key = ...  # compute a hash key here based on arguments
    return key

  @cachier(hash_func=calculate_hash)
  def calculate_super_complex_stuff(custom_obj):
    # amazing code goes here

See here for an example:

`Question: How to work with unhashable arguments <https://github.com/python-cachier/cachier/issues/91>`_


Precaching values
---------------------------------

If you want to load a value into the cache without calling the underlying function, this can be done with the `precache_value` function.

.. code-block:: python

  @cachier()
  def add(arg1, arg2):
    return arg1 + arg2

  add.precache_value(2, 2, value_to_cache=5)

  result = add(2, 2)
  print(result)  # prints 5


Per-function call arguments
---------------------------

Cachier also accepts several keyword arguments in the calls of the function it wraps rather than in the decorator call, allowing you to modify its behaviour for a specific function call.

Ignore Cache
~~~~~~~~~~~~

You can have ``cachier`` ignore any existing cache for a specific function call by passing ``ignore_cache=True`` to the function call. The cache will neither be checked nor updated with the new return value.

.. code-block:: python

  @cachier()
  def sum(first_num, second_num):
    return first_num + second_num

  def main():
    print(sum(5, 3, ignore_cache=True))

Overwrite Cache
~~~~~~~~~~~~~~~

You can have ``cachier`` overwrite an existing cache entry - if one exists - for a specific function call by passing ``overwrite_cache=True`` to the function call. The cache will not be checked but will be updated with the new return value.

Verbose Cache Call
~~~~~~~~~~~~~~~~~~

You can have ``cachier`` print out a detailed explanation of the logic of a specific call by passing ``verbose_cache=True`` to the function call. This can be useful if you are not sure why a certain function result is, or is not, returned.

Cache `None` Values
~~~~~~~~~~~~~~~~~~~

By default, ``cachier`` does not cache ``None`` values. You can override this behaviour by passing ``allow_none=True`` to the function call.


Cachier Cores
=============

Pickle Core
-----------

The default core for Cachier is pickle based, meaning each function will store its cache is a separate pickle file in the ``~/.cachier`` directory. Naturally, this kind of cache is both machine-specific and user-specific.

You can configure ``cachier`` to use another directory by providing the ``cache_dir`` parameter with the path to that directory:

.. code-block:: python

  @cachier(cache_dir='~/.temp/.cache')


You can slightly optimise pickle-based caching if you know your code will only be used in a single thread environment by setting:

.. code-block:: python

  @cachier(pickle_reload=False)

This will prevent reading the cache file on each cache read, speeding things up a bit, while also nullifying inter-thread functionality (the code is still thread safe, but different threads will have different versions of the cache at times, and will sometime make unnecessary function calls).

Setting the optional argument ``separate_files`` to ``True`` will cause the cache to be stored in several files: A file per argument set, per function. This can help if your per-function cache files become too large.

.. code-block:: python

  from cachier import cachier

  @cachier(separate_files=True)
  def foo(arg1, arg2):
    """Your function now has a persistent cache mapped by argument values, split across several files, per argument set"""
    return {'arg1': arg1, 'arg2': arg2}

You can get the fully qualified path to the directory of cache files used by ``cachier`` (``~/.cachier`` by default) by calling the ``cache_dpath()`` function:

.. code-block:: python

  >>> foo.cache_dpath()
      "/home/bigus/.cachier/"


MongoDB Core
------------
You can set a MongoDB-based cache by assigning ``mongetter`` with a callable that returns a ``pymongo.Collection`` object with writing permissions:

.. code-block:: python

    from pymongo import MongoClient

    def my_mongetter():
        client = MongoClient(get_cachier_db_auth_uri())
        db_obj = client['cachier_db']
        if 'someapp_cachier_db' not in db_obj.list_collection_names():
            db_obj.create_collection('someapp_cachier_db')
        return db_obj['someapp_cachier_db']

  @cachier(mongetter=my_mongetter)

This allows you to have a cross-machine, albeit slower, cache. This functionality requires that the installation of the ``pymongo`` python package.

In certain cases the MongoDB backend might leave a deadlock behind, blocking all subsequent requests from being processed. If you encounter this issue, supply the ``wait_for_calc_timeout`` with a reasonable number of seconds; calls will then wait at most this number of seconds before triggering a recalculation.

.. code-block:: python

  @cachier(mongetter=False, wait_for_calc_timeout=2)


Memory Core
-----------

You can set an in-memory cache by assigning the ``backend`` parameter with ``'memory'``:

.. code-block:: python

  @cachier(backend='memory')

Note, however, that ``cachier``'s in-memory core is simple, and has no monitoring or cap on cache size, and can thus lead to memory errors on large return values - it is mainly intended to be used with future multi-core functionality. As a rule, Python's built-in ``lru_cache`` is a much better stand-alone solution.


Contributing
============

Current maintainers are Shay Palachy Affek (`shay.palachy@gmail.com <mailto:shay.palachy@gmail.com>`_, `@shaypal5 <https://github.com/shaypal5>`_) and Judson Neer (`@lordjabez <https://github.com/lordjabez>`_); You are more than welcome to approach them for help. Contributions are very welcomed! :)

Installing for development
--------------------------

Clone:

.. code-block:: bash

  git clone git@github.com:python-cachier/cachier.git


Install in development mode with test dependencies:

.. code-block:: bash

  cd cachier
  pip install -e . -r tests/requirements.txt


Running the tests
-----------------

To run the tests, call the ``pytest`` command in the repository's root, or:

.. code-block:: bash

  python -m pytest

To run only MongoDB core related tests, use:

.. code-block:: bash

  pytest -m mongo

To run only memory core related tests, use:

.. code-block:: bash

  pytest -m memory

To run all tests EXCEPT MongoDB core related tests, use:

.. code-block:: bash

  pytest -m "not mongo"


To run all tests EXCEPT memory core AND MongoDB core related tests, use:

.. code-block:: bash

  pytest -m "not (mongo or memory)"


Running MongoDB tests against a live MongoDB instance
-----------------------------------------------------

**Note to developers:** By default, all MongoDB tests are run against a mocked MongoDB instance, provided by the ``pymongo_inmemory`` package. To run them against a live MongoDB instance, the ``CACHIER_TEST_VS_LIVE_MONGO`` environment variable is set to ``True`` in the ``test`` environment of this repository (and additional environment variables are populated with the appropriate credentials), used by the GitHub Action running tests on every commit and pull request.

Contributors are not expected to run these tests against a live MongoDB instance when developing, as credentials for the testing instance used will NOT be shared, but rather use the testing against the in-memory MongoDB instance as a good proxy.

**HOWEVER, the tests run against a live MongoDB instance when you submit a PR are the determining tests for deciding whether your code functions correctly against MongoDB.**


Adding documentation
--------------------

This project is documented using the `numpy docstring conventions`_, which were chosen as they are perhaps the most widely-spread conventions that are both supported by common tools such as Sphinx and result in human-readable docstrings (in my personal opinion, of course). When documenting code you add to this project, please follow `these conventions`_.

.. _`numpy docstring conventions`: https://github.com/numpy/numpy/blob/master/doc/HOWTO_DOCUMENT.rst.txt
.. _`these conventions`: https://github.com/numpy/numpy/blob/master/doc/HOWTO_DOCUMENT.rst.txt

Additionally, if you update this ``README.rst`` file, use ``python setup.py checkdocs`` to validate it compiles.


Credits
=======

Created by `Shay Palachy Affek <https://github.com/shaypal5>`_ (shay.palachy@gmail.com), which currently assists in maintenance.

Current lead developer/contributor: `Jirka Borovec <https://github.com/Borda>`_ (`@Borda <https://github.com/Borda>`_ on GitHub).

Other major contributors:

* `Jirka Borovec <https://github.com/Borda>`_ - Arg order independence, args-to-kwargs for less unique keys and numerous development and CI contributions.

* `Judson Neer <https://github.com/lordjabez>`_ - Precaching, method caching support and numerous improvements and bugfixes.

* `cthoyt <https://github.com/cthoyt>`_ - Base memory core implementation.

* `amarczew <https://github.com/amarczew>`_ - The ``hash_func`` kwarg.

* `non-senses <https://github.com/non-senses>`_ - The ``wait_for_calc_timeout`` kwarg.

* `Elad Rapaport <https://github.com/erap129>`_ - Multi-file Pickle core, a.k.a ``separate_files`` (released on ``v1.5.3``).

* `John Didion <https://github.com/jdidion>`_ - Support for pickle-based caching for cases where two identically-named methods of different classes are defined in the same module.

Notable bugfixers:

* `MichaelRazum <https://github.com/MichaelRazum>`_.

* `Eric Ma <https://github.com/ericmjl>`_ - The iNotify bugfix (released on ``v1.5.3``).

* `Ofir <https://github.com/ofirnk>`_ - The iNotify bugfix (released on ``v1.5.3``).



.. |PyPI-Status| image:: https://img.shields.io/pypi/v/cachier.svg
  :target: https://pypi.python.org/pypi/cachier

.. |PyPI-Versions| image:: https://img.shields.io/pypi/pyversions/cachier.svg
   :target: https://pypi.python.org/pypi/cachier

.. |Build-Status| image:: https://github.com/python-cachier/cachier/actions/workflows/test.yml/badge.svg
  :target: https://github.com/python-cachier/cachier/actions/workflows/test.yml

.. |LICENCE| image:: https://img.shields.io/pypi/l/cachier.svg
  :target: https://pypi.python.org/pypi/cachier

.. |Codecov| image:: https://codecov.io/github/python-cachier/cachier/coverage.svg?branch=master
   :target: https://codecov.io/github/python-cachier/cachier?branch=master

.. |Downloads| image:: https://pepy.tech/badge/cachier
     :target: https://pepy.tech/project/cachier
     :alt: PePy stats

.. |Codefactor| image:: https://www.codefactor.io/repository/github/python-cachier/cachier/badge?style=plastic
     :target: https://www.codefactor.io/repository/github/python-cachier/cachier
     :alt: Codefactor code quality

.. links:
.. _pymongo: https://api.mongodb.com/python/current/
.. _watchdog: https://github.com/gorakhargosh/watchdog
