""" Tests for models """
import six
import sys
import json
from datetime import datetime
from decimal import Decimal
from mock import patch, ANY
from dynamo3 import ItemUpdate

from flywheel import (Field, Composite, Model, NUMBER, GlobalIndex,
                      ConditionalCheckFailedException)
from flywheel.fields.types import UTC
from flywheel.tests import DynamoSystemTest
try:
    import unittest2 as unittest  # pylint: disable=F0401
except ImportError:
    import unittest
# pylint: disable=E1101


class Widget(Model):

    """ Test model with composite fields """
    __metadata__ = {
        'global_indexes': [
            GlobalIndex('ts-index', 'userid', 'ts').throughput(1, 1)
        ],
        'throughput': {
            'read': 1,
            'write': 1,
        },
    }
    userid = Field(hash_key=True)
    c_range = Composite('userid', 'id', range_key=True)
    c_index = Composite('userid', 'id', index='comp-index')
    c_plain = Composite('userid', 'id')
    _c_private = Composite('userid', 'id')
    id = Field()
    ts = Field(data_type=NUMBER)

    def __init__(self, userid, id, ts):
        super(Widget, self).__init__(userid, id=id, ts=ts)


class Post(Model):

    """ Test model with composite fields """
    __metadata__ = {
        'global_indexes': [
            GlobalIndex('score-index', 'c_all', 'score'),
        ]
    }
    hkey = Composite('userid', 'id', hash_key=True)
    userid = Field()
    id = Field()
    c_all = Composite('userid', 'id', 'about', 'text')
    score = Composite('likes', 'ts', 'deleted', data_type=NUMBER,
                      merge=lambda x, y, z: None if z else x + y)
    likes = Field(data_type=int, default=0)
    ts = Field(data_type=float, default=0)
    deleted = Field(data_type=bool, default=False)
    points = Field(data_type=Decimal, default=Decimal('0'))
    about = Field()
    text = Field()
    tags = Field(data_type=set)
    keywords = Composite('text', 'about', data_type=set,
                         merge=lambda t, a: t.split() + a.split(), coerce=True)

    def __init__(self, userid, id, ts, text='foo', about='bar'):
        super(Post, self).__init__(userid=userid, id=id, ts=ts, text=text,
                                   about=about)


class TestComposite(DynamoSystemTest):

    """ Tests for composite fields """
    models = [Widget, Post]

    def test_composite_field(self):
        """ Composite fields should be a combination of their components """
        w = Widget('a', 'b', 1)
        self.assertEquals(w.c_index, 'a:b')
        self.assertEquals(w.c_plain, 'a:b')

    def test_composite_store(self):
        """ Composite fields stored properly in dynamodb """
        w = Widget('a', 'b', 1)
        self.engine.save(w)
        tablename = Widget.meta_.ddb_tablename(self.engine.namespace)
        item = six.next(self.dynamo.scan(tablename))
        self.assertEquals(item['c_range'], w.c_range)
        self.assertEquals(item['c_index'], w.c_index)
        self.assertEquals(item['c_plain'], w.c_plain)

    def test_no_change_composite_hash(self):
        """ Changing the hash key raises an exception """
        w = Post('a', 'b', 1)
        self.engine.save(w)
        with self.assertRaises(AttributeError):
            w.userid = 'other'
        with self.assertRaises(AttributeError):
            w.id = 'other'

    def test_update_composite_fields(self):
        """ When updating a field all relevant composite fields are updated """
        w = Post('a', 'b', 1)
        self.engine.save(w)
        w.text = 'foobar'
        w.sync()
        tablename = w.meta_.ddb_tablename(self.engine.namespace)
        results = self.dynamo.batch_get(tablename,
                                        [{w.meta_.hash_key.name: w.hk_}])
        results = list(results)
        self.assertEquals(results[0]['text'], w.text)
        self.assertEquals(results[0]['c_all'], w.c_all)

    def test_composite_score(self):
        """ Composite score should be a combination of subfields """
        w = Post('a', 'a', 5)
        w.likes = 7
        self.assertEquals(w.score, 12)

    def test_update_composite_score(self):
        """ When updating a field, update score if necessary """
        w = Post('a', 'b', 4)
        self.engine.save(w)
        w.likes += 2
        w.sync()
        tablename = w.meta_.ddb_tablename(self.engine.namespace)
        results = self.dynamo.batch_get(tablename,
                                        [{w.meta_.hash_key.name: w.hk_}])
        results = list(results)
        self.assertEquals(results[0]['score'], 6)

    def test_set_composite_null(self):
        """ Composite fields can be set to None """
        p = Post('a', 'b', 2)
        self.engine.sync(p)
        self.assertEquals(p.score, 2)
        p.deleted = True

        p.sync()
        self.assertIsNone(p.score)
        result = self.engine(Post).filter(c_all=p.c_all)\
            .index('score-index').first()
        self.assertIsNone(result)

    def test_private_composite(self):
        """ Composite fields can be private """
        w = Widget('a', 'b', 1)
        self.engine.save(w)
        self.assertEqual(w.c_plain, w._c_private)


class Article(Model):

    """ Super simple test model """
    title = Field(hash_key=True)
    text = Field()
    views = Field(data_type=int)

    def __init__(self, title='Drugs win Drug War', **kwargs):
        super(Article, self).__init__(title, **kwargs)


class TestModelMutation(DynamoSystemTest):

    """ Tests for model mutation methods """
    models = [Post, Article]

    def test_save(self):
        """ Saving item puts it in the database """
        a = Article()
        self.engine.save(a)
        tablename = a.meta_.ddb_tablename(self.engine.namespace)
        result = six.next(self.dynamo.scan(tablename))
        self.assertEquals(result['title'], a.title)
        self.assertIsNone(result.get('text'))

    def test_save_conflict(self):
        """ Saving a duplicate item will raise an exception """
        a = Article(text='unfortunately')
        self.engine.save(a)
        a2 = Article(text='obviously')
        with self.assertRaises(ConditionalCheckFailedException):
            self.engine.save(a2, overwrite=False)

    def test_save_overwrite(self):
        """ Saving a duplicate item with overwrite=True overwrites existing """
        a = Article()
        self.engine.save(a)
        a2 = Article(text='obviously')
        self.engine.save(a2, overwrite=True)
        tablename = a.meta_.ddb_tablename(self.engine.namespace)
        result = six.next(self.dynamo.scan(tablename))
        self.assertEquals(result['title'], a2.title)
        self.assertEquals(result['text'], a2.text)

    def test_overwrite_all_fields(self):
        """ Save will clear existing, unspecified fields """
        a = Article(alpha='hi')
        self.engine.save(a)
        a2 = Article(beta='ih')
        self.engine.save(a2, overwrite=True)
        tablename = a.meta_.ddb_tablename(self.engine.namespace)
        result = six.next(self.dynamo.scan(tablename))
        self.assertEquals(result['title'], a2.title)
        self.assertEquals(json.loads(result['beta']), a2.beta)
        self.assertIsNone(result.get('alpha'))

    def test_save_conflict_extra(self):
        """ Save without overwrite raises error if unset fields differ """
        a = Article(alpha='hi')
        self.engine.save(a)
        a2 = Article(beta='ih')
        with self.assertRaises(ConditionalCheckFailedException):
            self.engine.save(a2, overwrite=False)

    def test_sync_new(self):
        """ Sync on a new item will create the item """
        p = Post('a', 'b', 4)
        self.engine.sync(p, raise_on_conflict=False)
        p2 = self.engine.scan(Post).first()
        self.assertEquals(p, p2)

    def test_conflict_sync_new(self):
        """ sync on a new item with raise_on_conflict=True creates item """
        p = Post('a', 'b', 4)
        self.engine.sync(p, raise_on_conflict=True)
        p2 = self.engine.scan(Post).first()
        self.assertEquals(p, p2)

    def test_sync_merges_fields(self):
        """ Syncing two new items with same pkey merges other fields """
        a = Article('a')
        a.author = 'me'
        self.engine.sync(a, raise_on_conflict=False)

        a2 = Article('a')
        a2.comments = 3
        self.engine.sync(a2, raise_on_conflict=False)

        self.assertEquals(a2.author, 'me')
        self.assertEquals(a2.comments, 3)

    def test_sync_only_updates_changed(self):
        """ Sync only updates fields that have been changed """
        with patch.object(self.engine, 'dynamo') as dynamo:
            captured_updates = []

            def update_item(_, __, updates, *___, **____):
                """ Mock update_item and capture the passed updateds """
                captured_updates.extend(updates)
                return {}
            dynamo.update_item.side_effect = update_item

            p = Post('a', 'b', 4)
            self.engine.save(p)
            p.foobar = set('a')
            p.ts = 4
            p.points = Decimal('2')
            p.sync(raise_on_conflict=False)
            self.assertEqual(len(captured_updates), 2)
            self.assertTrue(ItemUpdate.put('foobar', ANY) in captured_updates)
            self.assertTrue(ItemUpdate.put('points', ANY) in captured_updates)

    def test_sync_constraints(self):
        """ Sync can accept more complex constraints """
        p = Post('a', 'b', 4)
        self.engine.save(p)
        p.ts = 7
        p.sync(constraints=[Post.ts < 5])
        p2 = self.engine.scan(Post).first()
        self.assertEquals(p2.ts, 7)

    def test_sync_constraints_fail(self):
        """ Sync fails if complex constraints fail """
        p = Post('a', 'b', 4)
        self.engine.save(p)
        p.ts = 7
        with self.assertRaises(ConditionalCheckFailedException):
            p.sync(constraints=[Post.ts > 5])

    def test_sync_constraints_must_raise(self):
        """ Sync with constraints fails if raise_on_conflict is False """
        p = Post('a', 'b', 4)
        with self.assertRaises(ValueError):
            self.engine.sync(p, raise_on_conflict=False,
                             constraints=[Post.ts < 5])

    def test_delete(self):
        """ Model can delete itself """
        p = Post('a', 'b', 4)
        self.engine.save(p)
        p.delete()
        results = self.engine.scan(Post).all()
        self.assertEquals(results, [])

    def test_delete_no_conflict(self):
        """ Delete should delete item if no conflicts """
        p = Post('a', 'b', 4)
        self.engine.save(p)
        p.delete(raise_on_conflict=True)
        results = self.engine.scan(Post).all()
        self.assertEquals(results, [])

    def test_delete_conflict(self):
        """ Delete raise_on_conflict=True should raise exception on conflict """
        p = Post('a', 'b', 4)
        self.engine.save(p)
        p2 = self.engine.scan(Post).first()
        p.ts = 10
        p.sync()
        with self.assertRaises(ConditionalCheckFailedException):
            p2.delete(raise_on_conflict=True)

    def test_refresh(self):
        """ Refreshing model should refresh data """
        p = Post('a', 'b', 4)
        self.engine.save(p)
        p2 = self.engine.scan(Post).first()
        p.ts = 10
        p.sync()
        p2.refresh()
        self.assertEquals(p2.ts, p.ts)

    def test_refresh_multiple_models(self):
        """ Can refresh multiple model types """
        p = Post('a', 'b', 4)
        self.engine.save(p)
        p2 = self.engine.scan(Post).first()
        p2.ts = 10
        p2.sync()

        a = Article(text='unfortunately')
        self.engine.save(a)
        a2 = self.engine.scan(Article).first()
        a2.text = 'obviously'
        a2.sync()

        self.engine.refresh([a, p])
        self.assertEquals(p.ts, p2.ts)
        self.assertEquals(a.text, a2.text)

    def test_refresh_missing(self):
        """ Refreshing a set of models should work even if one is missing """
        p1 = Post('a', 'b', 4)
        p2 = Post('a', 'c', 5)
        p3 = Post('a', 'd', 6)
        self.engine.save([p1, p2])
        self.engine.refresh([p1, p2, p3])
        self.assertEqual(p1.id, 'b')
        self.assertEqual(p2.id, 'c')
        self.assertEqual(p3.id, 'd')
        self.assertEqual(p1.ts, 4)
        self.assertEqual(p2.ts, 5)
        self.assertEqual(p3.ts, 6)

    def test_sync_blank(self):
        """ Sync creates item even if only primary key is set """
        a = Article()
        self.engine.sync(a)
        tablename = a.meta_.ddb_tablename(self.engine.namespace)
        results = list(self.dynamo.scan(tablename))
        self.assertEquals(len(results), 1)
        result = dict(results[0])
        self.assertEquals(result, {
            'title': a.title,
        })

    def test_sync_no_conflict(self):
        """ Sync raise_on_conflict=True used just syncs object """
        p = Post('a', 'b', 4)
        self.engine.save(p)
        p.text = "hey"
        p.sync(raise_on_conflict=True)
        p2 = self.engine.scan(Post).first()
        self.assertEquals(p2.text, p.text)

    def test_sync_conflict(self):
        """ With sync raise_on_conflict=True, parallel writes raise error """
        p = Post('a', 'b', 4)
        p.foobar = "foo"
        self.engine.save(p)
        p2 = self.engine.scan(Post).first()
        p.foobar = "hey"
        p.sync()
        p2.foobar = "hi"
        with self.assertRaises(ConditionalCheckFailedException):
            p2.sync(raise_on_conflict=True)

    def test_sync_exist_conflict(self):
        """ When syncing, double-create raises error """
        p = Post('a', 'b', 4)
        self.engine.save(p)
        p2 = self.engine.scan(Post).first()
        p.foobar = "hey"
        p.sync()
        p2.foobar = "hi"
        with self.assertRaises(ConditionalCheckFailedException):
            p2.sync(raise_on_conflict=True)

    def test_sync_composite_conflict(self):
        """ Sync where composite key conflicts raises error """
        p = Post('a', 'b', 0, 'me', 'hi')
        self.engine.save(p)
        p2 = self.engine.scan(Post).first()
        p.about = "hey"
        p.sync()
        p2.text = "hey"
        with self.assertRaises(ConditionalCheckFailedException):
            p2.sync(raise_on_conflict=True)

    def test_sync_update(self):
        """ Sync should pull down most recent model """
        p = Post('a', 'b', 4)
        self.engine.save(p)
        p2 = self.engine.scan(Post).first()
        p.text = "hey"
        p.sync()
        p2.foobar = 'baz'
        p2.sync()
        self.assertEquals(p2.text, p.text)

    def test_sync_only_update(self):
        """ Sync should pull down most recent model even if no changes """
        p = Post('a', 'b', 4)
        self.engine.save(p)
        p2 = self.engine.scan(Post).first()
        p.text = "hey"
        p.sync()
        p2.sync()
        self.assertEquals(p2.text, p.text)

    def test_sync_update_delete(self):
        """ Sync should remove any attributes that have been deleted """
        p = Post('a', 'b', 4)
        p.foobar = 'baz'
        self.engine.save(p)
        p2 = self.engine.scan(Post).first()
        p.foobar = None
        p.sync()
        p2.sync()
        with self.assertRaises(AttributeError):
            _ = p2.foobar

    def test_incr(self):
        """ Parallel increments add """
        p = Post('a', 'b', 4)
        self.engine.save(p)
        p2 = self.engine.scan(Post).first()
        p.incr_(foobar=5)
        p.sync()
        p2.incr_(foobar=3)
        p2.sync(raise_on_conflict=False)
        self.assertEquals(p2.foobar, 8)

    def test_incr_float(self):
        """ Increment works on floats """
        p = Post('a', 'b', 4.5)
        self.engine.save(p)
        p.incr_(ts=5)
        self.assertEquals(p.ts, 9.5)
        p.sync()
        self.assertEquals(p.ts, 9.5)

    def test_incr_decimal(self):
        """ Increment works on Decimals """
        p = Post('a', 'b', 0)
        p.points = Decimal('1.5')
        self.engine.save(p)
        p.incr_(points=2)
        self.assertEquals(p.points, Decimal('3.5'))
        p.sync()
        self.assertEquals(p.points, Decimal('3.5'))
        self.assertTrue(isinstance(p.points, Decimal))

    def test_incr_no_conflict(self):
        """ Parallel increments with raise_on_conflict=True works """
        p = Post('a', 'b', 4)
        self.engine.save(p)
        p2 = self.engine.scan(Post).first()
        p.incr_(foobar=5)
        p.sync()
        p2.incr_(foobar=3)
        p2.sync(raise_on_conflict=True)
        self.assertEqual(p2.foobar, 8)

    def test_double_incr(self):
        """ Incrementing a field twice should work fine """
        p = Post('a', 'b', 4)
        p.foobar = 2
        self.engine.save(p)
        p.incr_(foobar=5)
        p.incr_(foobar=3)
        self.assertEquals(p.foobar, 10)
        p.sync()
        self.assertEquals(p.foobar, 10)

    def test_incr_set(self):
        """ Increment then set value raises exception """
        p = Post('a', 'b', 4)
        self.engine.save(p)
        p.incr_(foobar=7)
        with self.assertRaises(ValueError):
            p.foobar = 2

    def test_set_incr(self):
        """ Set value then increment raises exception """
        p = Post('a', 'b', 4)
        self.engine.save(p)
        p.foobar = 2
        with self.assertRaises(ValueError):
            p.incr_(foobar=5)

    def test_incr_read(self):
        """ Value changes immediately on incr """
        p = Post('a', 'b', 4)
        self.engine.save(p)
        p.incr_(ts=6, foobar=3)
        self.assertEquals(p.ts, 10)
        self.assertEquals(p.foobar, 3)

    def test_incr_unpersisted(self):
        """ Calling incr_ on unpersisted item merges with existing data """
        a = Article(views=2)
        self.engine.save(a)
        a = Article()
        a.incr_(views=4)
        self.engine.sync(a)
        a = self.engine.scan(Article).first()
        self.assertEqual(a.views, 6)

    def test_incr_unpersisted_overflow(self):
        """ Calling incr_ on unpersisted item overflow field merges data """
        a = Article()
        a.num = 2
        self.engine.save(a)
        a = Article()
        a.incr_(num=4)
        self.engine.sync(a)
        a = self.engine.scan(Article).first()
        self.assertEqual(a.num, 6)

    def test_incr_composite_piece(self):
        """ Incrementing a field will change any dependent composite fields """
        p = Post('a', 'b', 0)
        self.engine.save(p)
        p.incr_(likes=4)
        p.sync()

        tablename = p.meta_.ddb_tablename(self.engine.namespace)
        result = six.next(self.dynamo.scan(tablename))
        self.assertEquals(result['ts'], 0)
        self.assertEquals(result['likes'], 4)
        self.assertEquals(result['score'], 4)

    def test_incr_composite(self):
        """ Incr a field and sync changes any dependent fields """
        p = Post('a', 'b', 0)
        self.engine.save(p)
        p.incr_(likes=4)
        p.sync(raise_on_conflict=True)

        tablename = p.meta_.ddb_tablename(self.engine.namespace)
        result = six.next(self.dynamo.scan(tablename))
        self.assertEquals(result['ts'], 0)
        self.assertEquals(result['likes'], 4)
        self.assertEquals(result['score'], 4)

    def test_no_incr_primary_key(self):
        """ Cannot increment a primary key """
        p = Post('a', 'b', 0)
        self.engine.save(p)
        with self.assertRaises(AttributeError):
            p.incr_(userid=4)

    def test_no_incr_string(self):
        """ Cannot increment a string """
        p = Post('a', 'b', 0)
        self.engine.save(p)
        with self.assertRaises(TypeError):
            p.incr_(text='hi')

    def test_no_incr_composite(self):
        """ Cannot increment a composite field """
        p = Post('a', 'b', 0)
        self.engine.save(p)
        with self.assertRaises(TypeError):
            p.incr_(score=4)

    def test_delete_overflow_field(self):
        """ Can delete overflow fields by setting to None """
        p = Post('a', 'b', 0)
        p.foobar = 'hi'
        self.engine.save(p)
        p.foobar = None
        p.sync()

        tablename = p.meta_.ddb_tablename(self.engine.namespace)
        result = six.next(self.dynamo.scan(tablename))
        self.assertFalse('foobar' in result)

    def test_add_to_set(self):
        """ Adding a value to a set should be atomic """
        p = Post('a', 'b', 0)
        self.engine.save(p)
        p2 = self.engine.scan(Post).first()
        p.add_(tags='a')
        p2.add_(tags=set(['b', 'c']))
        p.sync()
        p2.sync()
        self.assertEqual(p2.tags, set(['a', 'b', 'c']))

    def test_add_to_set_conflict(self):
        """ Concurrent add to set with raise_on_conflict=True works """
        p = Post('a', 'b', 0)
        self.engine.save(p)
        p2 = self.engine.scan(Post).first()
        p.add_(tags='a')
        p2.add_(tags=set(['b', 'c']))
        p.sync()
        p2.sync(raise_on_conflict=True)
        self.assertEqual(p2.tags, set(['a', 'b', 'c']))

    def test_add_to_set_presync(self):
        """ Adding to a set should update local model value """
        p = Post('a', 'b', 0)
        p.add_(tags='a')
        self.assertEqual(p.tags, set(['a']))

    def test_dirty_requires_change(self):
        """ Don't mark fields dirty if the value hasn't changed """
        p = Post('a', 'b', 0)
        p.about = 'foobar'
        p.tags = set(['foo'])
        self.engine.save(p)
        p.about = 'foobar'
        p.tags = set(['foo'])
        self.assertEqual(p.__dirty__, set())

    def test_set_add_conflict(self):
        """ This is less a test and more documenting bad behavior """
        p = Post('a', 'b', 0)
        self.engine.save(p)
        # TODO: Right now if you add() and add_() to a set, the add()'s will be
        # ignored. It would be nice to at least provide a warning, but
        # preferably an error, when this happens.
        p.tags.add('foo')
        p.add_(tags='bar')
        p.sync()
        ret = self.engine.scan(Post).one()
        self.assertEqual(ret.tags, set(['bar']))

    def test_no_add_string(self):
        """ Cannot add_ to string fields """
        p = Post('a', 'b', 0)
        with self.assertRaises(TypeError):
            p.add_(about='something')

    def test_no_add_number(self):
        """ Cannot add_ to number fields """
        p = Post('a', 'b', 0)
        with self.assertRaises(TypeError):
            p.add_(likes=4)

    def test_no_add_composite(self):
        """ Cannot add_ to composite fields """
        p = Post('a', 'b', 0)
        with self.assertRaises(TypeError):
            p.add_(keywords=4)

    def test_no_add_and_set(self):
        """ Cannot both add_ to a set and set the value in same update """
        # Note that this behavior is only working on overflow fields ATM
        p = Post('a', 'b', 0)
        p.foobars = set(['a'])
        with self.assertRaises(ValueError):
            p.add_(foobars='b')

    def test_remove_from_set_presync(self):
        """ Removing from a set should update local model value """
        p = Post('a', 'b', 0)
        p.tags = set(['a', 'b', 'c'])
        self.engine.save(p)
        p.remove_(tags=set(['a', 'b']))
        self.assertEqual(p.tags, set(['c']))

    def test_remove_from_set(self):
        """ Removing values from a set should be atomic """
        p = Post('a', 'b', 0)
        p.tags = set(['a', 'b', 'c', 'd'])
        self.engine.save(p)
        p2 = self.engine.scan(Post).first()
        p.remove_(tags='a')
        p2.remove_(tags=set(['b', 'c']))
        p.sync()
        p2.sync()
        self.assertEqual(p2.tags, set(['d']))

    def test_add_to_overflow_set(self):
        """ Can atomically add to sets that are not declared Fields """
        p = Post('a', 'b', 0)
        p.add_(fooset='a')
        self.engine.save(p)
        p.add_(fooset='b')
        p.sync()
        ret = self.engine.scan(Post).one()
        self.assertEqual(ret.fooset, set(['a', 'b']))

    def test_remove_set_keyerror(self):
        """ Cannot remove missing elements from set """
        p = Post('a', 'b', 0)
        with self.assertRaises(KeyError):
            p.remove_(tags='a')

    def test_mutate_set_one_op(self):
        """ Can only atomically add or remove in a single update """
        p = Post('a', 'b', 0)
        p.add_(tags='a')
        with self.assertRaises(ValueError):
            p.remove_(tags='b')

    def test_mutate_set_smart_one_op(self):
        """ If adds/removes cancel out, throw no error """
        p = Post('a', 'b', 0)
        p.add_(tags='a')
        p.remove_(tags='a')
        self.assertEqual(p.tags, set())

    def test_delattr_field(self):
        """ Deleting a field sets it to None and deletes it from Dynamo """
        a = Article(publication='The Onion')
        self.engine.save(a)
        del a.text
        a.sync()
        stored_a = self.engine.scan(Article).first()
        self.assertIsNone(stored_a.text)

    def test_delattr_overflow_field(self):
        """ Deleting a field deletes it from Dynamo """
        a = Article(publication='The Onion')
        self.engine.save(a)
        del a.publication
        a.sync()
        stored_a = self.engine.scan(Article).first()
        self.assertIsNone(stored_a.get_('publication'))

    def test_delattr_private_field(self):
        """ Deleting a private field works like normal """
        a = Article()
        a._foobar = 'foobar'
        del a._foobar
        self.assertFalse(hasattr(a, '_foobar'))


class SetModel(Model):

    """ Test model with set """
    id = Field(hash_key=True)
    items = Field(data_type=set)


class TestDefaults(DynamoSystemTest):

    """ Test field defaults """
    models = [SetModel]

    def test_copy_mutable_field_default(self):
        """ Model fields should not share any mutable field defaults """
        m1 = SetModel('a')
        m1.items.add('foo')
        self.engine.save(m1)
        m2 = SetModel('b')
        self.assertTrue(m2.items is not m1.items)
        self.assertEqual(m2.items, set())


class Store(Model):

    """ Test model for indexes """
    __metadata__ = {
        'global_indexes': [
            GlobalIndex.all('name-index', 'name', 'city'),
            GlobalIndex.keys('name-emp-index', 'name', 'num_employees'),
            GlobalIndex.include('name-profit-index', 'name', 'monthly_profit',
                                includes=['name', 'num_employees']),
        ],
    }
    city = Field(hash_key=True)
    name = Field(range_key=True)
    sq_feet = Field(data_type=int).all_index('size-index')
    num_employees = Field(data_type=int).keys_index('emp-index')
    monthly_profit = Field(data_type=float)\
        .include_index('profit-index', ['name', 'num_employees'])


class TestCreate(DynamoSystemTest):

    """ Test model throughput settings """
    models = [Store]

    def tearDown(self):
        super(TestCreate, self).tearDown()
        Widget.meta_.delete_dynamo_schema(self.dynamo, wait=True)

    def _get_index(self, name):
        """ Get a specific index from the Store table """
        tablename = Store.meta_.ddb_tablename(self.engine.namespace)
        desc = self.dynamo.describe_table(tablename)
        for index in desc.indexes + desc.global_indexes:
            if index.name == name:
                return index

    def test_create_local_all_index(self):
        """ Create a local secondary ALL index """
        index = self._get_index('size-index')
        self.assertEquals(index.projection_type, 'ALL')

    def test_create_local_keys_index(self):
        """ Create a local secondary KEYS index """
        index = self._get_index('emp-index')
        self.assertEquals(index.projection_type, 'KEYS_ONLY')

    def test_create_local_include_index(self):
        """ Create a local secondary INCLUDE index """
        index = self._get_index('profit-index')
        self.assertEquals(index.projection_type, 'INCLUDE')
        self.assertEquals(index.include_fields, ['name', 'num_employees'])

    def test_create_global_all_index(self):
        """ Create a global secondary ALL index """
        index = self._get_index('name-index')
        self.assertEquals(index.projection_type, 'ALL')

    def test_create_global_keys_index(self):
        """ Create a global secondary KEYS index """
        index = self._get_index('name-emp-index')
        self.assertEquals(index.projection_type, 'KEYS_ONLY')

    def test_create_global_include_index(self):
        """ Create a global secondary INCLUDE index """
        index = self._get_index('name-profit-index')
        self.assertEquals(index.include_fields, ['name', 'num_employees'])

    def test_model_throughput(self):
        """ Model defines the throughput """
        Widget.meta_.create_dynamo_schema(self.dynamo, wait=True)
        tablename = Widget.meta_.ddb_tablename()
        desc = self.dynamo.describe_table(tablename)
        throughput = desc.throughput
        self.assertEquals(throughput.read, 1)
        self.assertEquals(throughput.write, 1)
        for index in desc.global_indexes:
            throughput = index.throughput
            self.assertEquals(throughput.read, 1)
            self.assertEquals(throughput.write, 1)

    def test_override_throughput(self):
        """ Throughput can be overridden in the create call """
        Widget.meta_.create_dynamo_schema(self.dynamo, wait=True, throughput={
            'read': 3,
            'write': 3,
            'ts-index': {
                'read': 3,
                'write': 3,
            },
        })
        tablename = Widget.meta_.ddb_tablename()
        desc = self.dynamo.describe_table(tablename)
        throughput = desc.throughput
        self.assertEquals(throughput.read, 3)
        self.assertEquals(throughput.write, 3)
        for index in desc.global_indexes:
            throughput = index.throughput
            self.assertEquals(throughput.read, 3)
            self.assertEquals(throughput.write, 3)


class TestModelMethods(unittest.TestCase):

    """ Unit tests for simple model operations """

    def test_comparison_with_none(self):
        """ Comparing a model to None should not throw exception """
        model = Article()
        self.assertNotEqual(model, None)


class Bare(Model):

    """ Bare-bones test model """

    id = Field(hash_key=True)
    score = Field(range_key=True, data_type=int)


class TestModelDefaults(unittest.TestCase):

    """ Test default model methods. """

    def test_default_constructor(self):
        """ Model should have a default constructor """
        m = Bare()
        self.assertIsNone(m.id)
        self.assertIsNone(m.score)

    def test_default_hash_key(self):
        """ Constructor can set hash key """
        m = Bare('a')
        self.assertEqual(m.id, 'a')
        self.assertIsNone(m.score)

    def test_default_range_key(self):
        """ Constructor can set range key """
        m = Bare('a', 5)
        self.assertEqual(m.id, 'a')
        self.assertEqual(m.score, 5)

    def test_constructor_kwargs(self):
        """ Can set any parameter with constructor kwargs """
        m = Bare(foo='bar')
        self.assertEqual(m.foo, 'bar')

    def test_too_many_args(self):
        """ Too many positional arguments to constructor raises error """
        with self.assertRaises(TypeError):
            Bare('a', 4, 5)

    def test_refresh_no_engine(self):
        """ Calling refresh() before model touches engine raises error """
        m = Bare('a', 1)
        with self.assertRaises(ValueError):
            m.refresh()

    def test_sync_no_engine(self):
        """ Calling sync() before model touches engine raises error """
        m = Bare('a', 1)
        with self.assertRaises(ValueError):
            m.sync()

    def test_delete_no_engine(self):
        """ Calling delete() before model touches engine raises error """
        m = Bare('a', 1)
        with self.assertRaises(ValueError):
            m.delete()

    def test_json(self):
        """ Model has default JSON serialization method """
        m = Bare('a', 1, foo='bar')
        js = m.__json__()
        self.assertEqual(js, {
            'id': 'a',
            'score': 1,
            'foo': 'bar',
        })

    def test_equality(self):
        """ Models have default equality method using primary key """
        m1 = Bare('a', 1)
        m2 = Bare('a', 1, foo='bar')
        self.assertEqual(m1, m2)
        self.assertEqual(hash(m1), hash(m2))

    def test_inequality(self):
        """ Models have default equality method using primary key """
        m1 = Bare('a', 1)
        m2 = Bare('a', 2)
        self.assertNotEqual(m1, m2)
        self.assertNotEqual(hash(m1), hash(m2))


class FloatModel(Model):
    """ Test model with floats in the primary key """
    hkey = Field(data_type=int, hash_key=True)
    rkey = Field(data_type=float, range_key=True)


class TestRefresh(DynamoSystemTest):

    """ Test model refresh """
    models = [FloatModel]

    def test_refresh_floating_point(self):
        """ Refresh with floats should not cause problems """
        p = FloatModel(4, 4.2932982983292)
        self.engine.save(p)
        p.refresh()
        # If there is a floating point mismatch, an error will be raised by now


class DatetimeModel(Model):
    """ Just something with a field that can raise when comparing """
    hkey = Field(data_type=int, hash_key=True)
    field = Field(data_type=datetime)


class ExplodingComparisons(DynamoSystemTest):

    """Make sure all comparisons are dealt with gracefully.

    This came up when comparing datetime objects with different TZ awareness,
    but applies to all error raises."""

    models = [DatetimeModel]

    def setUp(self):
        super(ExplodingComparisons, self).setUp()

        self.o = DatetimeModel(1, field=datetime.utcnow())
        self.engine.save(self.o)

    def test_ok(self):
        """ Happy case """
        self.o.field = datetime.utcnow()  # Same TZ awareness, should not raise.

    # Comaparing datetimes with == on 3.3 onwards doesn't raise.
    if sys.version_info[:2] < (3, 3):
        def test_kaboom(self):
            """ Sad case """
            now = datetime.utcnow().replace(tzinfo=UTC)

            # Prove to ourselves this explodes.
            with self.assertRaises(TypeError):
                # Because pylint was confused about not doing anything with the
                # =='s result
                bool(self.o.field == now)

            self.o.field = now
