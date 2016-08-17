.. _composite_fields:

Composite Fields
================
Composite fields allow you to create fields that are combinations of multiple
other fields. Suppose you're creating a table where you plan to store a
collection of social media items (tweets, facebook posts, instagram pics, etc).
If you make the hash key the id of the item, there is the remote possiblity
that a tweet id will collide with a facebook id. Here is the solution:

.. code-block:: python

    class SocialMediaItem(Model):
        userid = Field(hash_key=True)
        type = Field()
        id = Field()
        uid = Composite('type', 'id', range_key=True)

This will automatically generate a ``uid`` field from the values of ``type`` and ``id``. For example:

.. code-block:: python

    >>> item = SocialMediaItem(type='facebook', id='12345')
    >>> print item.uid
    facebook:12345

Note that setting a Composite field just doesn't work:

.. code-block:: python

    >>> item.uid = 'ILikeThisIDBetter'
    >>> print item.uid
    facebook:12345

By default, a Composite field simply joins its subfields with a ``':'``. You can
change that behavior for fancier applications:

.. code-block:: python

    def score_merge(likes, replies, deleted):
        if deleted:
            return None
        return likes + 5 * replies

    class Post(Model):
        userid = Field(hash_key=True)
        id = Field(range_key=True)
        likes = Field(type=int)
        replies = Field(type=int)
        deleted = Field(type=bool)
        score = Composite('likes', 'replies', 'deleted', type=int,
                          merge=score_merge, index='score-index')

So now you can update the ``likes`` or ``replies`` count, and the score will
automatically change. Which will re-arrange it in the index that you created.
Then, if you mark the post as "deleted", it will remove the score field which
removes it from the index.

*Whooooaaahh...*

The last neat little thing about Composite fields is how you can query them.
For numeric Composite fields you probably want to query directly on the score
like any other field. But if you're merging strings like with SocialMediaItem,
it can be cleaner to refer to the component fields themselves:

.. code-block:: python

    >>> fb_post = engine.query(SocialMediaItem).filter(userid='abc123',
    ...     type='facebook', id='12345').first()

The engine will automatically detect that you're trying to query on the range
key, and construct the ``uid`` from the pieces you provided.
