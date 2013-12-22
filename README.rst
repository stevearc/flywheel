flywheel
=========
Object mapper for Amazon's DynamoDB

TODO
====
* Documentation
* Indexes with different projections
* transactions
* Update boto Table.scan to take attributes=[] argument (for faster deletes)
* Cross-table linking (One and Many)

Notes
=====
Syncing fields that are part of a composite field is ONLY safe if you use atomic. Otherwise your data could become corrupted
corrollary: if you use incr on a field that is part of an atomic field, it will FORCE the sync to be atomic
