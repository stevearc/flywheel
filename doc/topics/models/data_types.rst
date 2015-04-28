.. _data_types:

Data Types
==========
DynamoDB supports three different data types: STRING, NUMBER, and BINARY. It
also supports sets of these types: STRING_SET, NUMBER_SET, BINARY_SET.

You can use these values directly for the model declarations, though they
require an import:

.. code-block:: python

    from flywheel import Model, Field, STRING, NUMBER

    class Tweet(Model):
        userid = Field(data_type=STRING, hash_key=True)
        id = Field(data_type=STRING, range_key=True)
        ts = Field(data_type=NUMBER, index='ts-index')
        text = Field(data_type=STRING)

There are other settings for data_type that are represented by python
primitives. Some of them (like ``unicode``) are functionally equivalent to the
DynamoDB option (``STRING``). Others, like ``int``, enforce an additional
application-level constraint on the data. Each option works transparently, so a
``datetime`` field would be set with ``datetime`` objects and you could query
against it using other ``datetime``'s.

Below is a table of python types, how they are stored in DynamoDB, and any
special notes. For more information, the code for data types is located in
:mod:`~flywheel.fields.types`.


+----------+----------+-------------+---------------------------------------------------------------+
| PY2 Type | PY3 Type | Dynamo Type | Description                                                   |
+==========+==========+=============+===============================================================+
| unicode  | str      | STRING      | Basic STRING type. This is the default for fields             |
+----------+----------+-------------+---------------------------------------------------------------+
| str      | bytes    | BINARY      | Binary data, (serialized objects, compressed data, etc)       |
+----------+----------+-------------+---------------------------------------------------------------+
| int/long | int      | NUMBER      | Enforces integer constraint on data                           |
+----------+----------+-------------+---------------------------------------------------------------+
| float    |          | NUMBER      |                                                               |
+----------+----------+-------------+---------------------------------------------------------------+
| Decimal  |          | NUMBER      |                                                               |
+----------+----------+-------------+---------------------------------------------------------------+
| set      |          | \*_SET      | This will use the appropriate type of DynamoDB set            |
+----------+----------+-------------+---------------------------------------------------------------+
| bool     |          | BOOL        |                                                               |
+----------+----------+-------------+---------------------------------------------------------------+
| datetime |          | NUMBER      | datetimes will be treated as naïve. UTC recommended.          |
+----------+----------+-------------+---------------------------------------------------------------+
| date     |          | NUMBER      | dates will be treated as naïve. UTC recommended.              |
+----------+----------+-------------+---------------------------------------------------------------+
| dict     |          | MAP         |                                                               |
+----------+----------+-------------+---------------------------------------------------------------+
| list     |          | LIST        |                                                               |
+----------+----------+-------------+---------------------------------------------------------------+

If you attempt to set a field with a type that doesn't match, it will raise a
``TypeError``.  If a field was created with ``coerce=True`` it will first
attempt to convert the value to the correct type. This means you could set an
``int`` field with the value ``"123"`` and it would perform the conversion for
you.

.. note::

    Certain fields will auto-coerce specific data types. For example, a
    ``bytes`` field will auto-encode a ``unicode`` to utf-8 even if
    ``coerce=False``.  Similarly, a ``unicode`` field will auto-decode a
    ``bytes`` value to a unicode string.

.. warning::

    If an ``int`` field is set to coerce values, it will still refuse to drop
    floating point data. This has the following effect:

.. code-block:: python

    >>> class Game(Model):
    ...    title = Field(hash_key=True)
    ...    points = Field(data_type=int, coerce=True)

    >>> mygame = Game()
    >>> mygame.points = 1.8
    ValueError: Field 'points' refusing to convert 1.8 to int! Results in data loss!

Set types
---------
If you define a ``set`` field with no additional parameters
``Field(data_type=set)``, flywheel will ensure that the field is a set, but
will perform no type checking on the items within the set. This should work
fine for basic uses when you are storing a number or string, but sets are able
to contain any data type listed in the table above (and any :ref:`custom type
<custom_data_type>` you declare). All you have to do is specify it in the
``data_type`` like so:

.. code-block:: python

    from flywheel import Model, Field, set_
    from datetime import date

    class Location(Model):
        name = Field(hash_key=True)
        events = Field(data_type=set_(date))

If you don't want to import ``set_``, you can use an equivalent expression with
the python ``frozenset`` builtin:

.. code-block:: python

    events = Field(data_type=frozenset([date]))

.. _custom_data_type:

Field Validation
----------------
You can apply one or more validators to a field. These are functions that
enforce some constraint on the field value beyond the type. Unlike the type
checking done above, the validation checks are only run when saving to the
database. An example:

.. code-block:: python

    class Widget(Model):
        id = Field(data_type=int, check=lambda x: x > 0)

To apply multiple validation checks, pass them in as a list or tuple:

.. code-block:: python

    def is_odd(x):
        return x % 2 == 1

    def is_natural(x):
        return x >= 0

    class Widget(Model):
        odd_natural_num = Field(data_type=int, check=(is_odd, is_natural))

There is a special case for enforcing that a field is non-null, since it is a
common case:

.. code-block:: python

    username = Field(nullable=False)

The ``nullable=False`` will generate an additional check to make sure the value
is non-null.

Custom Types
------------

You can define your own custom data types and make them available across all of
your models. All you need to do is create a subclass of
:class:`~flywheel.fields.types.TypeDefinition`. Let's make a type that will
store any python object in pickled format.

.. code-block:: python

    from flywheel.fields.types import TypeDefinition, BINARY, Binary
    import cPickle as pickle

    class PickleType(TypeDefinition):
        data_type = pickle #  name you use to reference this type
        aliases = ['pickle'] # alternate names that reference this type
        ddb_data_type = BINARY # data type of the field in dynamo

        def coerce(self, value, force):
            # Perform no type checking because we can pickle ANYTHING
            return value

        def ddb_dump(self, value):
            # Pickle and convert to a Binary object
            return Binary(pickle.dumps(value))

        def ddb_load(self, value):
            # Convert from a Binary object and unpickle
            return pickle.loads(value.value)

Now that you have your type definition, you can either use it directly in your code:

.. code-block:: python

    class MyModel(Model):
        myobj = Field(data_type=PickleType())


Or you can register it globally and reference it by its ``data_type`` or any
``aliases`` that were defined.

.. code-block:: python

    from flywheel.fields.types import register_type

    register_type(PickleType)

    class MyModel(Model):
        myobj = Field(data_type='pickle')
