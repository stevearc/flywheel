Getting Started
===============
To install flywheel, you will need to clone the repository and install the
package.  The easiest way to do this is using `devbox
<https://github.com/mathcamp/devbox>`_::

    wget https://raw.github.com/mathcamp/devbox/master/devbox/unbox.py && \
    python unbox.py git@github.com:mathcamp/flywheel

This will clone the repo and install it into a virtualenv named flywheel_env. You can now install flywheel into any virtualenv with:

.. code-block:: bash

    pip install path/to/flywheel

Here are the very first steps you need to take to get started with flywheel:

.. code-block:: python

    # Take care of some imports
    from datetime import datetime
    from flywheel import Model, Field, Engine

    # Set up our data model
    class Tweet(Model):
        userid = Field(hash_key=True)
        id = Field(range_key=True)
        ts = Field(data_type=datetime, index='ts-index')
        text = Field()

        def __init__(self, userid, id, ts, text):
            self.userid = userid
            self.id = id
            self.ts = ts
            self.text = text

    # Create an engine and connect to an AWS region
    engine = Engine()
    engine.connect_to_region('us-east-1',
                             aws_access_key_id=<YOUR AWS ACCESS KEY>,
                             aws_secret_access_key=<YOUR AWS SECRET KEY>)

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

Since DynamoDB has no schema, you can set arbitrary fields on the tweets:

.. code-block:: python

    tweet = Tweet('myuser', '1234', datetime.utcnow(), text='super rad')
    tweet.link = 'http://drmcninja.com'
    tweet.retweets = 0
    engine.save(tweet)

If you want to change a field, just make the change and sync it:

.. code-block:: python

    tweet.link = 'http://www.smbc-comics.com'
    tweet.sync()

That's enough to get you started. Look around if you'd like more details about :ref:`writing queries<queries>` or :ref:`how updates work<crud>`
