.. _metadata:

Metadata
========

Part of the model declaration is the ``__metadata__`` attribute, which is a
dict that configures the ``Model.meta_`` object. Models will inherit and merge
the __metadata__ fields from their ancestors. Keys that begin with an
underscore will not be merged. For example:

.. code-block:: python

    class Vehicle(Model):
        __metadata__ = {
            '_name': 'all-vehicles',
            'throughput': {
                'read': 10,
                'write': 2,
            }
        }

    class Car(Vehicle):
        pass

.. code-block:: python

    >>> print Car.__metadata__
    {'throughput': {'read': 10, 'write': 2}}

Below is a list of all the values that may be set in the ``__metadata__``
attribute of a model.

==============  =======  ===========
Key             Type     Description
==============  =======  ===========
_name           str      The name of the DynamoDB table (defaults to class name)
_abstract       bool     If True, no DynamoDB table will be created for this model (useful if you just want a class to inherit from)
throughput      dict     The table read/write throughput (defaults to {'read': 5, 'write': 5})
global_indexes  list     A list of GlobalIndex objects
==============  =======  ===========
