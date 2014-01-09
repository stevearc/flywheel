flywheel
=========
:Build: |build|_ |coverage|_
:Documentation: http://flywheel.readthedocs.org/en/latest/
:Source: https://github.com/mathcamp/flywheel

.. |build| image:: https://travis-ci.org/mathcamp/flywheel.png?branch=master
.. _build: https://travis-ci.org/mathcamp/flywheel
.. |coverage| image:: https://coveralls.io/repos/mathcamp/flywheel/badge.png?branch=master
.. _coverage: https://coveralls.io/r/mathcamp/flywheel?branch=master

Object mapper for Amazon's DynamoDB

Getting Started
===============
This is what a basic model looks like (schema taken from this `DynamoDB
API documentation
<http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/GSI.html>`_)
::

    from flywheel import Model, Field, GlobalIndex

    class GameScore(Model):
        __metadata__ = {
            'global_indexes': [
                GlobalIndex('GameTitleIndex', 'title', 'top_score')
            ],
        }
        userid = Field(hash_key=True)
        title = Field(range_key=True)
        top_score = Field(data_type=int)
        top_score_time = Field(data_type=datetime)
        wins = Field(data_type=int)
        losses = Field(data_type=int)

        def __init__(self, title, userid):
            self.title = title
            self.userid = userid

Create a new top score::

    >>> score = GameScore('Master Blaster', 'abc')
    >>> score.top_score = 9001
    >>> score.top_score_time = datetime.utcnow()
    >>> engine.sync(score)

Get all top scores for a user::

    >>> scores = engine.query(GameScore).filter(userid='abc').all()

Get the top score for Galaxy Invaders::

    >>> top_score = engine.query(GameScore).filter(title='Galaxy Invaders')\
    ...     .first(desc=True)

Atomically increment a user's "wins" count on Alien Adventure::

    >>> score = self.engine.get(GameScore, userid='abc', title='Alien Adventure')
    >>> score.incr_(wins=1)
    >>> score.sync()

Get all scores on Comet Quest that are over 9000::

    >>> scores = engine.query(GameScore).filter(GameScore.top_score > 9000,
    ...                                         title='Comet Quest').all()


Development
===========
To get started developing flywheel, run the following command::

    wget https://raw.github.com/mathcamp/devbox/master/devbox/unbox.py && \
    python unbox.py git@github.com:mathcamp/flywheel

This will clone the repository and install the package into a virtualenv

Running Tests
-------------
The command to run tests is ``python setup.py nosetests``. Most of these tests
require `DynamoDB Local
<http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Tools.html>`_.
There is a nose plugin that will download and run the DynamoDB Local service
during the tests. It requires the java 6/7 runtime, so make sure you have that
installed.
