Changelog
=========
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
