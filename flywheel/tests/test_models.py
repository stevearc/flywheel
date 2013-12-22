""" Tests for models """
from boto.dynamodb2.exceptions import ConditionalCheckFailedException

from . import BaseSystemTest
from flywheel import Field, Composite, Model, NUMBER, GlobalIndex


class Widget(Model):

    """ Test model with composite fields """
    __metadata__ = {
        'global_indexes': [
            GlobalIndex('ts-index', 'userid', 'ts',
                        throughput={'read': 1, 'write': 1}),
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
    id = Field()
    ts = Field(data_type=NUMBER)

    def __init__(self, userid, id, ts):
        self.userid = userid
        self.id = id
        self.ts = ts


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
    score = Composite('likes', 'ts', data_type=NUMBER,
                      merge=lambda x, y: x + y)
    likes = Field(data_type=NUMBER)
    ts = Field(data_type=NUMBER)
    about = Field()
    text = Field()

    def __init__(self, userid, id, ts, text='foo', about='bar'):
        self.userid = userid
        self.id = id
        self.ts = ts
        self.text = text
        self.about = about


class Article(Model):

    """ Super simple test model """
    title = Field(hash_key=True)
    text = Field()

    def __init__(self, title):
        self.title = title


class TestComposite(BaseSystemTest):

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
        table = w.meta_.ddb_table(self.dynamo)
        item = list(table.scan())[0]
        self.assertEquals(dict(item),
                          {'userid': w.userid,
                           'ts': w.ts,
                           'id': w.id,
                           'c_range': w.c_range,
                           'c_index': w.c_index,
                           'c_plain': w.c_plain,
                           })

    def test_no_change_composite_hash(self):
        """ Changing the hash key raises an exception """
        w = Post('a', 'b', 1)
        self.engine.save(w)
        with self.assertRaises(AttributeError):
            w.userid = 'other'
        with self.assertRaises(AttributeError):
            w.id = 'other'

    def test_update_composite_fields(self):
        """ When updating a field, all relevant composite fields are updated """
        w = Post('a', 'b', 1)
        self.engine.save(w)
        w.text = 'foobar'
        w.sync()
        table = w.meta_.ddb_table(self.dynamo)
        results = table.batch_get(keys=[{w.meta_.hash_key.name: w.hk_}])
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
        table = w.meta_.ddb_table(self.dynamo)
        results = table.batch_get(keys=[{w.meta_.hash_key.name: w.hk_}])
        results = list(results)
        self.assertEquals(results[0]['score'], 6)


class TestThroughput(BaseSystemTest):

    """ Test model throughput settings """

    def tearDown(self):
        super(TestThroughput, self).tearDown()
        Widget.meta_.namespace = ['test']
        Widget.meta_.delete_dynamo_schema(self.dynamo, wait=True)

    def test_model_throughput(self):
        """ Model defines the throughput """
        Widget.meta_.create_dynamo_schema(self.dynamo, wait=True)
        desc = self.dynamo.describe_table(Widget.meta_.ddb_tablename)
        throughput = desc['Table']['ProvisionedThroughput']
        self.assertEquals(throughput['ReadCapacityUnits'], 1)
        self.assertEquals(throughput['WriteCapacityUnits'], 1)
        global_indexes = desc['Table']['GlobalSecondaryIndexes']
        for index in global_indexes:
            throughput = index['ProvisionedThroughput']
            self.assertEquals(throughput['ReadCapacityUnits'], 1)
            self.assertEquals(throughput['WriteCapacityUnits'], 1)

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
        desc = self.dynamo.describe_table(Widget.meta_.ddb_tablename)
        throughput = desc['Table']['ProvisionedThroughput']
        self.assertEquals(throughput['ReadCapacityUnits'], 3)
        self.assertEquals(throughput['WriteCapacityUnits'], 3)
        global_indexes = desc['Table']['GlobalSecondaryIndexes']
        for index in global_indexes:
            throughput = index['ProvisionedThroughput']
            self.assertEquals(throughput['ReadCapacityUnits'], 3)
            self.assertEquals(throughput['WriteCapacityUnits'], 3)


class TestModelMutation(BaseSystemTest):

    """ Tests for model mutation methods """
    models = [Post, Article]

    def test_sync_new(self):
        """ Sync on a new item will create the item """
        p = Post('a', 'b', 4)
        self.engine.sync(p, atomic=False)
        p2 = self.engine.scan(Post).first()
        self.assertEquals(p, p2)

    def test_atomic_sync_new(self):
        """ Atomic sync on a new item will create the item """
        p = Post('a', 'b', 4)
        self.engine.sync(p, atomic=True)
        p2 = self.engine.scan(Post).first()
        self.assertEquals(p, p2)

    def test_sync_merges_fields(self):
        """ Syncing two new items with same pkey merges other fields """
        a = Article('a')
        a.author = 'me'
        self.engine.sync(a, atomic=True)

        a2 = Article('a')
        a2.comments = 3
        self.engine.sync(a2, atomic=True)

        self.assertEquals(a2.author, 'me')
        self.assertEquals(a2.comments, 3)

    def test_delete(self):
        """ Model can delete itself """
        p = Post('a', 'b', 4)
        self.engine.save(p)
        p.delete()
        results = self.engine.scan(Post).all()
        self.assertEquals(results, [])

    def test_atomic_sync(self):
        """ Atomic sync used normally just syncs object """
        p = Post('a', 'b', 4)
        self.engine.save(p)
        p.text = "hey"
        p.sync(atomic=True)
        p2 = self.engine.scan(Post).first()
        self.assertEquals(p2.text, p.text)

    def test_atomic_sync_error(self):
        """ When doing an atomic sync, parallel writes raise error """
        p = Post('a', 'b', 4)
        p.foobar = "foo"
        self.engine.save(p)
        p2 = self.engine.scan(Post).first()
        p.foobar = "hey"
        p.sync()
        p2.foobar = "hi"
        with self.assertRaises(ConditionalCheckFailedException):
            p2.sync(atomic=True)

    def test_atomic_sync_error_exist(self):
        """ When doing an atomic sync, double-create raises error """
        p = Post('a', 'b', 4)
        self.engine.save(p)
        p2 = self.engine.scan(Post).first()
        p.foobar = "hey"
        p.sync()
        p2.foobar = "hi"
        with self.assertRaises(ConditionalCheckFailedException):
            p2.sync(atomic=True)

    def test_atomic_sync_composite_conflict(self):
        """ Atomic sync where composite key conflicts raises error """
        p = Post('a', 'b', 0, 'me', 'hi')
        self.engine.save(p)
        p2 = self.engine.scan(Post).first()
        p.about = "hey"
        p.sync()
        p2.text = "hey"
        with self.assertRaises(ConditionalCheckFailedException):
            p2.sync(atomic=True)

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
        p2.sync(atomic=False)
        self.assertEquals(p2.foobar, 8)

    def test_incr_atomic(self):
        """ Parallel increments with atomic=True raises exception """
        p = Post('a', 'b', 4)
        self.engine.save(p)
        p2 = self.engine.scan(Post).first()
        p.incr_(foobar=5)
        p.sync()
        p2.incr_(foobar=3)
        with self.assertRaises(ConditionalCheckFailedException):
            p2.sync(atomic=True)

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

    def test_incr_composite(self):
        """ Incrementing a field will change any dependent composite fields """
        p = Post('a', 'b', 0)
        self.engine.save(p)
        p.incr_(likes=4)
        p.sync()
        result = self.engine.scan(Post).first(attributes=['score', 'likes',
                                                          'ts'])
        self.assertEquals(result['ts'], 0)
        self.assertEquals(result['likes'], 4)
        self.assertEquals(result['score'], 4)

    def test_incr_composite_atomic(self):
        """ Incr a field and atomic sync changes any dependent fields """
        p = Post('a', 'b', 0)
        self.engine.save(p)
        p.incr_(likes=4)
        p.sync(atomic=True)
        result = self.engine.scan(Post).first(attributes=['score', 'likes',
                                                          'ts'])
        self.assertEquals(result['ts'], 0)
        self.assertEquals(result['likes'], 4)
        self.assertEquals(result['score'], 4)
