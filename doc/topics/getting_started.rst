Getting Started
===============
Flywheel can be installed with pip

.. code-block:: bash

    pip install flywheel

Here are the steps to set up a simple example model with flywheel:

.. code-block:: python

    # Take care of some imports
    from datetime import datetime
    from flywheel import Model, Field, Engine

    # Set up our data model
    class Tweet(Model):
        userid = Field(hash_key=True)
        id = Field(range_key=True)
        ts = Field(type=datetime, index='ts-index')
        text = Field()

        def __init__(self, userid, id, ts, text):
            self.userid = userid
            self.id = id
            self.ts = ts
            self.text = text

    # Create an engine and connect to an AWS region
    engine = Engine()
    engine.connect_to_region('us-east-1')

    # Register our model with the engine so it can create the Dynamo table
    engine.register(Tweet)

    # Create the dynamo table for our registered model
    engine.create_schema()

Now that you have your model, your engine, and the Dynamo table, you can begin
adding tweets:

.. code-block:: python

    tweet = Tweet('myuser', '1234', datetime.utcnow(), text='@awscloud hey '
                  'I found this cool new python library for AWS...')
    engine.save(tweet)

To get data back out, query it using the engine:

.. code-block:: python

    # Get the 10 most recent tweets by 'myuser'
    recent = engine.query(Tweet)\
            .filter(Tweet.ts <= datetime.utcnow(), userid='myuser')\
            .limit(10).all(desc=True)

    # Get a specific tweet by a user
    tweet = engine.query(Tweet).filter(userid='myuser', id='1234').first()

If you want to change a field, just make the change and sync it:

.. code-block:: python

    tweet.text = 'This tweet has been removed due to shameless promotion'
    tweet.sync()

That's enough to give you a taste. The rest of the docs have more information
on :ref:`creating models <model_basics>`, :ref:`writing queries<queries>`, or :ref:`how
updates work<crud>`.
