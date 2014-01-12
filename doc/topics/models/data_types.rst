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
special notes. For more information, the code for data types is located at
:mod:`~flywheel.fields.types`.


+----------+-------------+---------------------------------------------------------------+
| Type     | Dynamo Type | Description                                                   |
+==========+=============+===============================================================+
| unicode  | STRING      | Basic STRING type. This is the default for fields             |
+----------+-------------+---------------------------------------------------------------+
| str      | BINARY      |                                                               |
+----------+-------------+---------------------------------------------------------------+
| int      | NUMBER      | Enforces integer constraint on data                           |
+----------+-------------+---------------------------------------------------------------+
| float    | NUMBER      |                                                               |
+----------+-------------+---------------------------------------------------------------+
| set      | \*_SET      | This will use the appropriate type of DynamoDB set            |
+----------+-------------+---------------------------------------------------------------+
| bool     | NUMBER      |                                                               |
+----------+-------------+---------------------------------------------------------------+
| datetime | NUMBER      | datetimes MUST be provided in UTC                             |
+----------+-------------+---------------------------------------------------------------+
| date     | NUMBER      | dates MUST be provided in UTC                                 |
+----------+-------------+---------------------------------------------------------------+
| Decimal  | NUMBER      | If you need decimal precision in your application             |
+----------+-------------+---------------------------------------------------------------+
| dict     | STRING      | Stored as json-encoded string                                 |
+----------+-------------+---------------------------------------------------------------+
| list     | STRING      | Stored as json-encoded string                                 |
+----------+-------------+---------------------------------------------------------------+
| S3Type   | STRING      | Stores the S3 key path as a string                            |
+----------+-------------+---------------------------------------------------------------+

If you attempt to set a field with a type that doesn't match, it will raise a
``ValueError``.  If a field was created with ``coerce=True`` it will first
attempt to convert the value to the correct type. This means you could set an
``int`` field with the value ``"123"`` and it would perform the conversion for
you.

**Exception #1**: Even if ``coerce=False``, a ``str`` field will auto-encode a
``unicode`` value using ``utf-8``. Similarly, a ``unicode`` field will
auto-decode a ``str`` value using ``utf-8``.

**Exception #2**: If an ``int`` field is set to coerce values, it will still
refuse to drop floating point data. Your application should never discard data
without being explicitly told to. This has the following effect:

.. code-block:: python

    >>> class Game(Model):
    ...    title = Field(hash_key=True)
    ...    points = Field(data_type=int, coerce=True)

    >>> mygame = Game()
    >>> mygame.points = 1.8
    ValueError: Field 'points' refusing to convert 1.8 to int! Results in data loss!

Advanced Types
--------------

S3 Keys
^^^^^^^
You can use :class:`~flywheel.fields.types.S3Type` to quickly and easily
reference S3 values from your model objects. This type will store the S3 key in
Dynamo and put a :class:`~boto.s3.key.Key` object in your model.

.. code-block:: python

    from flywheel.fields.types import S3Type

    class Image(Model):
        user = Field(hash_key=True)
        name = Field(range_key=True)
        taken = Field(data_type=datetime, index='taken-index')
        data = Composite('user', 'name', data_type=S3Type('my_image_bucket'),
                         merge=lambda *a: '/'.join(a))

        def __init__(self, user, name, taken):
            self.user = user
            self.name = name
            self.taken = taken

You can use this class like so:

.. code-block:: python

    >>> img = Image('Rob', 'Big Sur.jpg', datetime.utcnow())
    >>> img.data.set_contents_from_filename(img.name)
    >>> engine.save(img)

It will store the image data in the S3 bucket named ``my_image_bucket`` and use
the path ``Rob/Big Sur.jpg``. See :ref:`composite_fields` for more about how
the key path is generated.
