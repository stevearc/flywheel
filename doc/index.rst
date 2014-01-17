Flywheel - Object Mapper for DynamoDB
=====================================
Flywheel is a library for mapping python objects to DynamoDB tables. It uses a
SQLAlchemy-like syntax for queries.

User Guide
----------

.. toctree::
    :maxdepth: 2
    :glob:

    topics/getting_started
    topics/models
    topics/queries
    topics/crud
    topics/develop

API Reference
-------------
.. toctree::
    :maxdepth: 3
    :glob:

    ref/flywheel

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

TODO
====
* query objects should be iterable
* release to pypi
* Cross-table linking (One and Many)
* Fields should be able to set nullable=False
