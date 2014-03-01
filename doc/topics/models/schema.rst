.. _schema:

Schema
======
There are four main key concepts to understanding a DynamoDB table.

**Hash key**: This field will be sharded. Pick something with relatively random
access (e.g. userid is good, timestamp is bad)

**Range key**: Optional. This field will be indexed, so you can query against
it (within a specific hash key).

The hash key and range key together make the **Primary key**, which is the
unique identifier for each object.

**Local Secondary Indexes**: Optional, up to 5. You may only use these if your
table has a range key. These fields are indexed in a similar fashion as the
range key. You may also query against them within a specific hash key. You can
think of these as range keys with no uniqueness requirements.

**Global Secondary Indexes**: Optional, up to 5. These indexes have a hash key
and optional range key, and can be put on any declared field. This allows you
to shard your tables by more than one value.

For additional information on table design, read the `AWS docs on best
practices
<http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/BestPractices.html>`_

Example declaration of hash and range key:

.. code-block:: python

    class Tweet(Model):
        userid = Field(hash_key=True)
        ts = Field(data_type=datetime, range_key=True)

For this version of a Tweet, each ``(userid, ts)`` pair is a unique value. The
Dynamo table will be sharded across userids.

Local Secondary Indexes
-----------------------
Indexes also have a Projection Type. Creating an index requires duplicating
some amount of data in the storage, and the projection type allows you to
optimize how much additional storage is used. The projection types are:

**All**: All fields are projected into the index

**Keys only**: Only the primary key and indexed keys are projected into the index

**Include**: Like the "keys only" projection, but allows you to specify
additional fields to project into the index

This is how they it looks in the model declaration:

.. code-block:: python

    class Tweet(Model):
        userid = Field(hash_key=True)
        id = Field(range_key=True)
        ts = Field(data_type=datetime).all_index('ts-index')
        retweets = Field(data_type=int).keys_index('rt-index')
        likes = Field(data_type=int).include_index('like-index', ['text'])
        text = Field()

The default index projection is "All", so you could replace the ``ts`` field
above with:

.. code-block:: python

    ts = Field(data_type=datetime, index='ts-index')

Global Secondary Indexes
------------------------
Like their Local counterparts, Global Secondary Indexes can specify a
projection type. Unlike their Local counterparts, Global Secondary Indexes are
provisioned with a *separate* read/write throughput from the base table. This
can be specified in the model declaration. Here are some examples below:

.. code-block:: python

    class Tweet(Model):
        __metadata__ = {
            'global_indexes': [
                GlobalIndex.all('ts-index', 'city', 'ts').throughput(read=10, write=2),
                GlobalIndex.keys('rt-index', 'city', 'retweets')\
                        .throughput(read=10, write=2),
                GlobalIndex.include('like-index', 'city', 'likes',
                                    includes=['text']).throughput(read=10, write=2),
            ],
        }
        userid = Field(hash_key=True)
        city = Field()
        id = Field(range_key=True)
        ts = Field(data_type=datetime)
        retweets = Field(data_type=int)
        likes = Field(data_type=int)
        text = Field()

If you want more on indexes, check out the `AWS docs on indexes
<http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/SecondaryIndexes.html>`_.
