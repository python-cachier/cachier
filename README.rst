Cachier
=======

Persistent, stale-free cache / memoization decorators for Python.

.. code-block:: python

  from cachier import cachier
  import datetime
  
  SHELF_LIFE = datetime.timedelta(days=3)
  
  @cachier(stale_after=SHELF_LIFE)
  def foo(arg1, arg2):
    """foo now has a persistent cache, trigerring recalculation for values stored more than 3 days!"""
    return {'arg1': arg1, 'arg2': arg2}
    

Dependencies and Setup
----------------------

s3bp uses the following packages:

* pymongo's `bson package`_
* watchdog_

You can install cachier using:

.. code-block:: python

    pip install cachier

Features
----------------------

* A simple interface.
* Defining "shelf life" for cached values.
* Local caching using pickle files.
* Cross-machine caching using MongoDB.
* Thread-safety.

Cachier is not:

* Meant as a transient cache. Python's @lru_cache is better.
* Especially fast. It is meant to replace function calls that take more than... a second, say (overhead is around 1 millisecond).


Use
---

Pickle-based Caching
~~~~~~~~~~~~~~~~~~~~
You can add a deafult, pickle-based persistent cache to your function by decorating it with the ``cachier`` decorator (notice the ``()``!).

.. code-block:: python

  from cachier import cachier
  
  @cachier()
  def foo(arg1, arg2):
    """Your function now has a persistent cache mapped by argument values!"""
    return {'arg1': arg1, 'arg2': arg2}

Setting Shelf Live
~~~~~~~~~~~~~~~~~~~~
You can set any duration as the shelf life of cached return values of a function by providing a corresponding ``timedelta`` object to the ``stale_after`` parameter:

.. code-block:: python

  import datetime
  
  @cachier(stale_after=datetime.timedelta(weeks=2)
  def bar(arg1, arg2):
    return {'arg1': arg1, 'arg2': arg2}

.. links:
.. _bson package: https://api.mongodb.com/python/current/api/bson/
.. _watchdog: https://github.com/gorakhargosh/watchdog
