.. _model_basics:

Model Basics
============
This is what a model looks like:

.. code-block:: python

    class Tweet(Model):
        userid = Field(hash_key=True)
        id = Field(range_key=True)
        ts = Field(data_type=datetime, index='ts-index')
        text = Field()

The model declares the fields an object has, their :ref:`data
types<data_types>`, and the :ref:`schema <schema>` of the table.

Since DynamoDB is a NoSQL database, you can attach arbitrary additional fields
to the model, and they will be stored appropriately. For example, this tweet
doesn't declare a ``retweets`` field, but you could assign it anyway:

.. code-block:: python

    tweet = Tweet()
    tweet.retweets = 7

Since models define the schema of a table, you can use them to create or delete
tables. Every model has a ``meta_`` field attached to it which contains
metadata about the model. This metadata object has the :meth:`create
<flywheel.models.ModelMetadata.create_dynamo_schema>` and :meth:`delete
<flywheel.models.ModelMetadata.delete_dynamo_schema>` methods.

.. code-block:: python

    from boto.dynamodb2 import connect_to_region

    connection = connect_to_region('us-east-1')
    Tweet.meta_.create_dynamo_schema(connection)

You can also register your models with the engine and create all the tables at once:

.. code-block:: python

    engine.register(User, Tweet, Message)
    engine.create_schema()

When you define your model, you can specify the throughput it will be
provisioned with when created:

.. code-block:: python

    class Tweet(Model):
        __metadata__ = {
            'throughput': {
                'read': 10,
                'write': 5,
            },
        }
