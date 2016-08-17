.. _queries:

Table Queries
=============
The query syntax is heavily inspired by `SQLAlchemy <http://www.sqlalchemy.org/>`_.
In DynamoDB, queries must use one of the table's indexes. Queries are
constrained to a single hash key value. This means that for a query there will
always be at least one call to ``filter`` which will, at a minimum, set the
hash key to search on.

.. code-block:: python

    # Fetch all tweets made by a user
    engine.query(Tweet).filter(Tweet.userid == 'abc123').all()

You may also use inequality filters on range keys and secondary indexes

.. code-block:: python

    # Fetch all tweets made by a user in the past day
    earlyts = datetime.utcnow() - timedelta(days=1)
    engine.query(Tweet).filter(Tweet.userid == 'abc123',
                               Tweet.ts >= earlyts).all()

There are two finalizing statements that will return all results:
:meth:`~flywheel.engine.Query.all` and :meth:`~flywheel.engine.Query.gen`.
Calling :meth:`~flywheel.engine.Query.all` will return a list of results.
Calling :meth:`~flywheel.engine.Query.gen` will return a generator. If your
query will return a large number of results, using
:meth:`~flywheel.engine.Query.gen` can help you avoid storing them all in
memory at the same time.

.. code-block:: python

    # Count how many retweets a user has in total
    retweets = 0
    all_tweets = engine.query(Tweet).filter(Tweet.userid == 'abc123').gen()
    for tweet in all_tweets:
        retweets += tweet.retweets

There are two finalizing statements that retrieve a single item:
:meth:`~flywheel.engine.Query.first` and :meth:`~flywheel.engine.Query.one`.
Calling :meth:`~flywheel.engine.Query.first` will return the first element of
the results, or None if there are no results. Calling
:meth:`~flywheel.engine.Query.one` will return the first element of the results
*only if* there is *exactly* one result. If there are no results or more
results it will raise a :class:`ValueError`.

.. code-block:: python

    # Get a single tweet by a user
    tweet = engine.query(Tweet).filter(Tweet.userid == 'abc123').first()

    # Get a specific tweet and fail if missing
    tweet = engine.query(Tweet).filter(Tweet.userid == 'abc123',
                                       Tweet.id == '1234').one()

There is one more finalizing statement: :meth:`~flywheel.engine.Query.count`.
This will return the number of results that matched the query, instead of
returning the results themselves.

.. code-block:: python

    # Get the number of tweets made by user abc123
    num = engine.query(Tweet).filter(Tweet.userid == 'abc123').count()

You can set a :meth:`~flywheel.engine.Query.limit` on a query to limit the
number of results it returns:

.. code-block:: python

    # Get the first 10 tweets by a user after a timestamp
    afterts = datetime.utcnow() - timedelta(hours=1)
    tweets = engine.query(Tweet).filter(Tweet.userid == 'abc123',
                                        Tweet.ts >= afterts).limit(10).all()

One way to delete items from a table is with a query. Calling
:meth:`~flywheel.engine.Query.delete` will delete all items that match a query:

.. code-block:: python

    # Delete all of a user's tweets older than 1 year
    oldts = datetime.utcnow() - timedelta(days=365)
    engine.query(Tweet).filter(Tweet.userid == 'abc123',
                               Tweet.ts < oldts).delete()

Most of the time the query engine will be able to automatically detect which
local or global secondary index you intend to use. If the index is ambiguous,
you can manually specify the index. This can also be useful if you want the
results to be sorted by a particular index when only querying the hash key.

.. code-block:: python

    # This is the schema for the following example
    class Tweet(Model):
        userid = Field(hash_key=True)
        id = Field(range_key=True)
        ts = Field(type=datetime, index='ts-index')
        retweets = Field(type=int, index='rt-index')

    # This returns 10 tweets in id order (more-or-less random)
    ten_tweets = engine.query(Tweet).filter(userid='abc123').limit(10).all()

    # Get the 10 most retweeted tweets for a user
    top_ten = engine.query(Tweet).filter(userid='abc123').index('rt-index')\
            .limit(10).all(desc=True)

    # Get The 10 most recent tweets for a user
    ten_recent = engine.query(Tweet).filter(userid='abc123').index('ts-index')\
            .limit(10).all(desc=True)

**New in 0.2.1**

Queries can filter on fields that are not the hash or range key. Filtering this
way will strip out the results server-side, but it will not use an index. When
filtering on these extra fields, you may use the additional filter operations
that are listed under :ref:`scan`.

Shorthand
---------
If you want to avoid typing 'query' everywhere, you can simply call the engine:

.. code-block:: python

    # Long form query
    engine.query(Tweet).filter(Tweet.userid == 'abc123').all()

    # Abbreviated query
    engine(Tweet).filter(Tweet.userid == 'abc123').all()

Filter constraints with ``==`` can be instead passed in as keyword arguments:

.. code-block:: python

    # Abbreviated filter
    engine(Tweet).filter(userid='abc123').all()

    engine(Tweet).filter(userid='abc123', id='1234').first()

You can still pass in other constraints as positional arguments to the same
filter:

.. code-block:: python

    # Multiple filters in same statement
    engine(Tweet).filter(Tweet.ts <= earlyts, userid='abc123').all()

.. _scan:

Table Scans
-----------
Table scans are similar to table queries, but they do not use an index. This
means they have to read every item in the table. This is EXTREMELY SLOW. The
benefit is that they do not have to filter based on the hash key, and they have
a few additional filter arguments that may be used.

.. code-block:: python

    # Fetch all tweets ever
    alltweets = engine.scan(Tweet).gen()

    # Fetch all tweets that tag awscloud
    tagged = engine.scan(Tweet).filter(Tweet.tags.contains_('awscloud')).all()

    # Fetch all tweets with annoying, predictable text
    annoying = set(['first post', 'hey guys', 'LOOK AT MY CAT'])
    first = engine.scan(Tweets).filter(Tweet.text.in_(annoying)).all()

    # Fetch all tweets with a link
    linked = engine.scan(Tweet).filter(Tweet.link != None).all()

Since table scans don't use indexes, you can filter on fields that are not
declared in the model. Here are some examples:

.. code-block:: python

    # Fetch all tweets that link to wikipedia
    educational = engine.scan(Tweet)\
            .filter(Tweet.field_('link').beginswith_('http://wikipedia')).all()

    # You can also use the keyword arguments to filter
    best_tweets = engine.scan(Tweet)\
            .filter(link='http://en.wikipedia.org/wiki/Morgan_freeman').all()
