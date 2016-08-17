.. _model_basics:

Model Basics
============
This is what a model looks like:

.. code-block:: python

    class Tweet(Model):
        userid = Field(hash_key=True)
        id = Field(range_key=True)
        ts = Field(type=datetime, index='ts-index')
        text = Field()

The model declares the fields an object has, their :ref:`data
types<data_types>`, and the :ref:`schema <schema>` of the table.

Since DynamoDB is a NoSQL database, you can attach arbitrary additional fields
(undeclared fields) to the model, and they will be stored appropriately. For
example, this tweet doesn't declare a ``retweets`` field, but you could assign
it anyway:

.. code-block:: python

    tweet.retweets = 7
    tweet.sync()

Undeclared fields will **not** be saved if they begin or end with an
underscore. This is intentional behavior so you can set local-only variables on
your models.

.. code-block:: python

    tweet.retweets = 7  # this is saved to Dynamo
    tweet._last_updated = datetime.utcnow()  # this is NOT saved to Dynamo

Since models define the schema of a table, you can use them to create or delete
tables. Every model has a ``meta_`` field attached to it which contains
metadata about the model. This metadata object has the :meth:`create
<flywheel.models.ModelMetadata.create_dynamo_schema>` and :meth:`delete
<flywheel.models.ModelMetadata.delete_dynamo_schema>` methods.

.. code-block:: python

    from dynamo3 import DynamoDBConnection

    connection = DynamoDBConnection.connect_to_region('us-east-1')
    Tweet.meta_.create_dynamo_schema(connection)
    Tweet.meta_.delete_dynamo_schema(connection)

You can also register your models with the engine and create all the tables at once:

.. code-block:: python

    engine.register(User, Tweet, Message)
    engine.create_schema()
