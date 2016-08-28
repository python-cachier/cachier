Cachier
=======

Persistent, stale-free cache / memoization decorators for Python.

.. code-block:: python

  from cachier import cachier
  
  @cachier()
  def foo(arg1, arg2):
    """Your function now has a persistent cache mapped by argument values!"""
    return {'arg1': arg1, 'arg2': arg2}
    

Dependencies and Setup
----------------------

s3bp uses the following packages:

* pymongo's `bson package`_
* watchdog_

You can install cachier using:

.. code-block:: python

    pip install cachier


Use
---

Pickle-based Caching
~~~~~~~~~~~~~~~~~~~~
You can add a deafult, pickle-based persistent cache to your function by decorating int with the `cachier` decorator (notice the `()`!).

.. code-block:: python

  from cachier import cachier
  
  @cachier()
  def foo(arg1, arg2):
    """Your function now has a persistent cache mapped by argument values!"""
    return {'arg1': arg1, 'arg2': arg2}



.. links:
.. _bson package: https://api.mongodb.com/python/current/api/bson/
.. _watchdog: https://github.com/gorakhargosh/watchdog
