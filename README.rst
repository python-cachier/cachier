Cachier
#######

|PyPI-Status| |Downloads| |PyPI-Versions| |Build-Status| |Codecov| |Codefactor| |LICENCE|

Persistent, stale-free, local and cross-machine caching for Python functions.

.. code-block:: python

  from cachier import cachier
  import datetime

  @cachier(stale_after=datetime.timedelta(days=3))
  def foo(arg1, arg2):
    """foo now has a persistent cache, trigerring recalculation for values stored more than 3 days."""
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
* Compatible with Python 3.5+ (and Python 2.7 up until version 1.2.8). 
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

You can get the fully qualified path to the directory of cache files used by ``cachier`` (``~/.cachier`` by default) by calling the ``cache_dpath()`` function:

.. code-block:: python

  >>> foo.cache_dpath()
      "/home/bigus/.cachier/"


Resetting a Cache
-----------------
The Cachier wrapper adds a ``clear_cache()`` function to each wrapped function. To reset the cache of the wrapped function simply call this method:

.. code-block:: python

  foo.clear_cache()


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

As mentioned above, the positional and keyword arguments to the wrapped function must be hashable (i.e. Python's immutable built-in objects, not mutable containers). To get around this limitation the ``hash_params`` parameter of the ``cachier`` decorator can be provided with a callable that gets the args and kwargs from the decorated function and returns a hash key for them.

.. code-block:: python

  @cachier(hash_params=hash_my_custom_class)
  def calculate_super_complex_stuff(custom_obj):
    # amazing code goes here


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


MongoDB Core
------------
You can set a MongoDB-based cache by assigning ``mongetter`` with a callable that returns a ``pymongo.Collection`` object with writing permissions:

.. code-block:: python

  @cachier(mongetter=False)

This allows you to have a cross-machine, albeit slower, cache. This functionality requires that the installation of the ``pymongo`` python package.


Contributing
============

Package author and current maintainer is Shay Palachy (shay.palachy@gmail.com); You are more than welcome to approach him for help. Contributions are very welcomed.

Installing for development
--------------------------

Clone:

.. code-block:: bash

  git clone git@github.com:shaypal5/cachier.git


Install in development mode with test dependencies:

.. code-block:: bash

  cd cachier
  pip install -e ".[test]"


Running the tests
-----------------

To run the tests, use:

.. code-block:: bash

  python -m pytest --cov=cachier


Adding documentation
--------------------

This project is documented using the `numpy docstring conventions`_, which were chosen as they are perhaps the most widely-spread conventions that are both supported by common tools such as Sphinx and result in human-readable docstrings (in my personal opinion, of course). When documenting code you add to this project, please follow `these conventions`_.

.. _`numpy docstring conventions`: https://github.com/numpy/numpy/blob/master/doc/HOWTO_DOCUMENT.rst.txt
.. _`these conventions`: https://github.com/numpy/numpy/blob/master/doc/HOWTO_DOCUMENT.rst.txt


Credits
=======
Created by Shay Palachy (shay.palachy@gmail.com).

.. Contributers (in chronological order of first commit):

.. * `shaypal5 <https://github.com/shaypal5>`_ (Shay Palachy)
.. * `j-chad <https://github.com/j-chad>`_ (Jackson)



.. |PyPI-Status| image:: https://img.shields.io/pypi/v/cachier.svg
  :target: https://pypi.python.org/pypi/cachier

.. |PyPI-Versions| image:: https://img.shields.io/pypi/pyversions/cachier.svg
   :target: https://pypi.python.org/pypi/cachier

.. |Build-Status| image:: https://travis-ci.org/shaypal5/cachier.svg?branch=master
  :target: https://travis-ci.org/shaypal5/cachier

.. |LICENCE| image:: https://img.shields.io/pypi/l/cachier.svg
  :target: https://pypi.python.org/pypi/cachier

.. |Codecov| image:: https://codecov.io/github/shaypal5/cachier/coverage.svg?branch=master
   :target: https://codecov.io/github/shaypal5/cachier?branch=master

.. |Downloads| image:: https://pepy.tech/badge/cachier
     :target: https://pepy.tech/project/cachier
     :alt: PePy stats
     
.. |Codefactor| image:: https://www.codefactor.io/repository/github/shaypal5/cachier/badge?style=plastic
     :target: https://www.codefactor.io/repository/github/shaypal5/cachier
     :alt: Codefactor code quality

.. links:
.. _pymongo: https://api.mongodb.com/python/current/
.. _watchdog: https://github.com/gorakhargosh/watchdog
