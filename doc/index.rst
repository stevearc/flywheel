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
* rename the 'atomic' parameter to something clearer
* release to pypi
* Cross-table linking (One and Many)
* Sets of custom data types (e.g. dates)
