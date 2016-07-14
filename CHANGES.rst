Changelog
=========

0.4.9
-----
* Feature: Add ``save()`` method to Models (:issue:`40`)
* Feature: Add ``update_field()`` method to Engine (:issue:`43`)

0.4.8
-----
* Bug fix: Bad function call in ``index_pk_dict_``

0.4.7
-----
* New ``index_pk_dict_`` method for constructing `exclusive_start_key` for index queries (:issue:`34`)

0.4.6
-----
* Pass exclusive_start_key through to dynamo3 (:issue:`34`)

0.4.5
-----
* Bug fix: Calling refresh() could sometimes crash from unordered results.

0.4.4
-----
* Bug fix: Mutable field defaults are no longer shared among model instances

0.4.3
-----
* Bug fix: Incorrect ``ConditionalCheckFailedException`` when syncing changes to a Composite field.
* Allow ``DateTimeType`` to be stored as a naive datetime.

0.4.2
-----
* Make the ``dict``, ``list``, and ``bool`` types backwards-compatible with the old json-serialized format (:pr:`24`)
* Allow queries to use ``in``, ``not null``, and a few other constraints that were missing (:sha:`8b8854d`)
* Models are smarter about marking fields as dirty for sync (:issue:`26`)
* Stopped using deprecated ``expected`` syntax for dynamo3

0.4.1
-----
* **Warning**: Stored datetime objects will now be timezone-aware (:sha:`a7c253d`)
* **Warning**: Stored datetime objects will now keep their microseconds (:sha:`fffe92c`)

0.4.0
-----
* **Breakage**: Dropping support for python 3.2 due to lack of botocore support
* **Breakage**: Changing the ``list``, ``dict``, and ``bool`` data types to use native DynamoDB types instead of JSON serializing
* **Breakage** and bug fix: Fixing serialization of ``datetime`` and ``date`` objects (for more info see the commit) (:sha:`df049af`)
* Feature: Can now do 'contains' filters on lists
* Feature: Fields support multiple validation checks
* Feature: Fields have an easy way to enforce non-null values (``nullable=False``)

Data type changes are due to an `update in the DynamoDB API
<https://aws.amazon.com/blogs/aws/dynamodb-update-json-and-more/>`_

0.3.0
-----
* **Breakage**: Engine namespace is slightly different. If you pass in a string it will be used as the table name prefix with no additional '-' added.

0.2.1
-----
* **Breakage**: Certain queries may now require you to specify an index where it was auto-detected before
* Feature: Queries can now filter on non-indexed fields
* Feature: More powerful "sync-if" constraints
* Feature: Can OR together filter constraints in queries

All changes are due to an `update in the DynamoDB API
<http://aws.amazon.com/blogs/aws/improved-queries-and-updates-for-dynamodb/>`_

0.2.0
-----
* **Breakage**: Engine no longer accepts boto connections (using dynamo3 instead)
* **Breakage**: Removing S3Type (no longer have boto as dependency)
* Feature: Support Python 3.2 and 3.3
* Feature: ``.count()`` terminator for queries (:sha:`bf3261c`)
* Feature: Can override throughputs in ``Engine.create_schema()`` (:sha:`4d1abe0`)
* Bug fix: Engine ``namespace`` is truly isolated (:sha:`3b4fad7`)

0.1.3
-----
* Bug fix: Some queries fail when global index has no range key (:issue:`9`, :sha:`edce6e2`)

0.1.2
-----
* Bug fix: Field names can begin with an underscore (:sha:`637f1ee`, :issue:`7`)
* Feature: Models have a nice default __init__ method (:sha:`40068c2`)

0.1.1
-----
* Bug fix: Can call ``incr_()`` on models that have not been saved yet (:sha:`0a1990f`)
* Bug fix: Model comparison with ``None`` (:sha:`374dda1`)

0.1.0
-----
* First public release
