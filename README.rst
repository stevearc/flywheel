flywheel
=========
:Build: |build|_ |coverage|_
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
        top_score_time = Field()
        wins = Field(data_type=int)
        losses = Field(data_type=int)

        def __init__(self, title, userid):
            self.title = title
            self.userid = userid

Create a new top score::

    >>> score = GameScore('Master Blaster', 'abc')
    >>> score.top_score = 9001
    >>> score.top_score_time = datetime.utcnow().isoformat()
    >>> engine.sync(score)

Get all top scores for a user::

    >>> scores = engine.query(GameScore).filter(userid='abc').all()

Get the top score for Galaxy Invaders::

    >>> top_score = engine.query(GameScore).filter(title='Galaxy Invaders')\
    ...     .first(reverse=True)

Safely increment the 'wins' count for Alien Adventure with no chance of
failure::

    >>> score = GameScore('Alien Adventure', 'abc')
    >>> score.incr_(wins=1)
    >>> engine.sync(score, atomic=False)

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
The command to run tests is ``python setup.py nosetests``. Some of these tests
require `DynamoDB Local
<http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Tools.html>`_.
There is a nose plugin that will download and run the DynamoDB Local service
during the tests. It requires the java 6/7 runtime, so make sure you have that
installed.

TODO
====
* Add date field
* Sync should be able to create blank objects (how does save(overwrite=True) work?)
* Test index creation
* Indexes with different projections
* Documentation
* migration engine
* Update boto Table.scan to take attributes=[] argument (for faster deletes)
* Cross-table linking (One and Many)

Notes
=====
* Syncing fields that are part of a composite field is ONLY safe if you use atomic. Otherwise your data could become corrupted
* corrollary: if you use incr on a field that is part of an atomic field, it will FORCE the sync to be atomic
* Syncing when only primary keys are set will do nothing
* datetime types must be utc
