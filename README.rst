Flywheel
========
:Build: |build|_ |coverage|_
:Documentation: http://flywheel.readthedocs.org/
:Downloads: http://pypi.python.org/pypi/flywheel
:Source: https://github.com/stevearc/flywheel

.. |build| image:: https://travis-ci.org/stevearc/flywheel.png?branch=master
.. _build: https://travis-ci.org/stevearc/flywheel
.. |coverage| image:: https://coveralls.io/repos/stevearc/flywheel/badge.png?branch=master
.. _coverage: https://coveralls.io/r/stevearc/flywheel?branch=master

Object mapper for Amazon's DynamoDB

Getting Started
===============
This is what a basic model looks like (schema taken from this `DynamoDB
API documentation
<http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/GSI.html>`_)

.. sourcecode:: python

    from flywheel import Model, Field, GlobalIndex

    class GameScore(Model):
        __metadata__ = {
            'global_indexes': [
                GlobalIndex('GameTitleIndex', 'title', 'top_score')
            ],
        }
        userid = Field(hash_key=True)
        title = Field(range_key=True)
        top_score = Field(type=int)
        top_score_time = Field(type=datetime)
        wins = Field(type=int)
        losses = Field(type=int)

        def __init__(self, title, userid):
            self.title = title
            self.userid = userid

Create a new top score

.. sourcecode:: python

    >>> score = GameScore('Master Blaster', 'abc')
    >>> score.top_score = 9001
    >>> score.top_score_time = datetime.utcnow()
    >>> engine.sync(score)

Get all top scores for a user

.. sourcecode:: python

    >>> scores = engine.query(GameScore).filter(userid='abc').all()

Get the top score for Galaxy Invaders

.. sourcecode:: python

    >>> top_score = engine.query(GameScore).filter(title='Galaxy Invaders')\
    ...     .first(desc=True)

Atomically increment a user's "wins" count on Alien Adventure

.. sourcecode:: python

    >>> score = GameScore('Alien Adventure', 'abc')
    >>> score.incr_(wins=1)
    >>> engine.sync(score)

Get all scores on Comet Quest that are over 9000

.. sourcecode:: python

    >>> scores = engine.query(GameScore).filter(GameScore.top_score > 9000,
    ...                                         title='Comet Quest').all()
