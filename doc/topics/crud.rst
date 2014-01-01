.. _crud:

CRUD
====
This section covers the operations you can do to save, read, update, and delete
items from the database. All of these methods exist on the
:class:`~flywheel.engine.Engine` object and can be called on one or many items.
After being saved-to or loaded-from Dynamo, the items themselves will have
these methods attached to them as well. For example, these are both valid:

.. code-block:: python

    >>> engine.sync(tweet)
    >>> tweet.sync()

Save
----
Save the item to Dynamo. This is intended for new items that were just created
and need to be added to the database. If you ``save()`` an item that already
exists in Dynamo, it will raise an exception. You may optionally use
``save(overwrite=True)`` to instead clobber existing data and write your
version of the item to Dynamo.

.. code-block:: python

    >>> tweet = Tweet()
    >>> engine.save(tweet)
    >>> tweet.text = "Let's replace the whole item"
    >>> tweet.save(overwrite=True)

Refresh
-------
Query dynamo to get the most up-to-date version of a model. Clobbers any
existing data on the item. To force a consistent read use
``refresh(consistent=True)``.

This call is very useful if you query indexes that use an incomplete projection
type. The results won't have all of the item's fields, so you can call
``refresh()`` to get any attributes that weren't projected onto the index.

.. code-block:: python

    >>> tweet = engine.query(Tweet).filter(userid='abc123')\
    ...         .index('ts-index').first(desc=True)
    >>> tweet.refresh()

Get
---
Fetch an item from its primary key fields. This will be faster than a query,
but requires you to know the primary key/keys of all items you want fetched.

.. code-block:: python

    >>> my_tweet = engine.get(Tweet, userid='abc123', id='1')

You can also fetch many at a time:

.. code-block:: python

    >>> key1 = {'userid': 'abc123', 'id': '1'}
    >>> key2 = {'userid': 'abc123', 'id': '2'}
    >>> key3 = {'userid': 'abc123', 'id': '3'}
    >>> some_tweets = engine.get(Tweet, [key1, key2, key3])

Delete
------
Deletes an item. You may pass in ``delete(atomic=True)``, which will only
delete the item if none of the values have changed since it was read.

.. code-block:: python

    >>> tweet = engine.query(Tweet).filter(userid='abc123', id='123').first()
    >>> tweet.delete()

You may also delete an item from a primary key specification:

.. code-block:: python

    >>> engine.delete_key(Tweet, userid='abc123', id='1')

And you may delete many at once:

.. code-block:: python

    >>> key1 = {'userid': 'abc123', 'id': '1'}
    >>> key2 = {'userid': 'abc123', 'id': '2'}
    >>> key3 = {'userid': 'abc123', 'id': '3'}
    >>> engine.delete_key(Tweet, [key1, key2, key3])

Sync
----
Save any fields that have been changed on an item. This will update changed
fields in Dynamo and ensure that all fields exactly reflect the item in the
database.  This is usually used for updates, but it can be used to create new
items as well.

.. code-block:: python

    >>> tweet = Tweet()
    >>> engine.sync(tweet)
    >>> tweet.text = "Update just this field"
    >>> tweet.sync()

Models will automatically detect changes to mutable fields, such as ``dict``,
``list``, and ``set``.

.. code-block:: python

    >>> tweet.tags.add('awscloud')
    >>> tweet.sync()

Since sync does a partial update, it can tolerate concurrent writes of
different fields.

.. code-block:: python

    >>> tweet = engine.query(Tweet).filter(userid='abc123', id='1234').first()
    >>> tweet2 = engine.query(Tweet).filter(userid='abc123', id='1234').first()
    >>> tweet.author = "The Pope"
    >>> tweet.sync()
    >>> tweet2.text = "Mo' money mo' problems"
    >>> tweet2.sync() #  it works!
    >>> print tweet2.author
    The Pope

This "merge" behavior is also what happens when you ``sync()`` items to create
them. If the item to create already exists in Dynamo, that's fine as long as
there are no conflicting fields. Note that this behavior is distinctly
different from ``save()``, so make sure you pick the right call for your use
case.

Atomic Sync
^^^^^^^^^^^
If you use ``sync(atomic=True)``, the sync operation will check that every
field that you're updating has not been changed since you last read it. This is
very useful for preventing concurrent writes.

.. warning::

    If you change a key that is part of a :ref:`composite
    field<composite_fields>`, you should **always** sync with ``atomic=True``.
    If you don't, you run the risk of corrupting the value of the composite
    field.

Atomic Increment
^^^^^^^^^^^^^^^^
DynamoDB supports truly atomic increment/decrement of NUMBER fields. To use
this functionality, there is a special call you need to make:


.. code-block:: python

    >>> # Increment the number of retweets by 1
    >>> tweet.incr_(retweets=1)
    >>> tweet.sync()

BOOM.

.. warning::

    Due to the weirdness with composite fields listed above, if you increment a
    field that is part of a composite field, flywheel will **force** the sync
    to be atomic. This guarantees that using ``incr_()`` will always be safe.

Default Atomic Behavior
-----------------------
You can configure the default behavior for each of these endpoints using
:attr:`~flywheel.engine.Engine.default_atomic`. The default setting will cause
``sync()`` to be atomic, ``delete()`` not to be, and ``save()`` will overwrite.
Check the attribute docs for more options. You can, of course, pass in the
argument to the calls directly to override this behavior on a case-by-case
basis.
