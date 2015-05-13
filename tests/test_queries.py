""" Tests for engine queries """
from six.moves import xrange as _xrange  # pylint: disable=F0401
from flywheel import (Field, Composite, Model, NUMBER, STRING_SET, GlobalIndex,
                      DuplicateEntityException, EntityNotFoundException)
from flywheel.tests import DynamoSystemTest


class User(Model):

    """ Model for testing queries """
    __metadata__ = {
        'global_indexes': [
            GlobalIndex('name-index', 'name', 'score'),
        ],
    }
    id = Field(hash_key=True)
    name = Field(range_key=True)
    score = Field(data_type=NUMBER, index='score-index', default=0)
    str_set = Field(data_type=STRING_SET)
    bio = Field()
    plan = Field()


def score_merge(ts, upvotes):
    """ Merge the ts and upvotes """
    return ts + 1000 * upvotes


class Post(Model):

    """ Model for testing composite queries """
    __metadata__ = {
        'global_indexes': [
            GlobalIndex('name-index', 'username', 'score'),
            GlobalIndex('ts-index', 'username', 'ts'),
            GlobalIndex('hash-index', 'total_uid')
        ],
    }
    uid = Composite('type', 'id', hash_key=True)
    type = Field()
    id = Field()
    score = Composite('ts', 'upvotes', range_key=True, data_type=NUMBER,
                      merge=score_merge)
    username = Field()
    ts = Field(data_type=NUMBER, default=0)
    upvotes = Field(data_type=NUMBER, default=0)
    total_uid = Composite('uid', 'username', merge=lambda x, y: x + ':' +
                          str(y))


class TestQueries(DynamoSystemTest):

    """ Tests for table queries """
    models = [User]

    def test_first(self):
        """ Query can retrieve first element of results """
        u = User(id='a', name='Adam')
        self.engine.save(u)
        result = self.engine(User).filter(id='a').first()
        self.assertEquals(result, u)

    def test_first_none(self):
        """ If no results, first() returns None """
        result = self.engine(User).filter(id='a').first()
        self.assertIsNone(result)

    def test_one(self):
        """ Query can retrieve first element of results """
        u = User(id='a', name='Adam')
        self.engine.save(u)
        result = self.engine(User).filter(id='a').one()
        self.assertEquals(result, u)

    def test_one_many(self):
        """ If many results, one() raises DuplicateEntityException """
        u = User(id='a', name='Adam')
        u2 = User(id='a', name='Aaron')
        self.engine.save([u, u2])
        # For legacy reasons, make sure it also is a ValueError.
        with self.assertRaises(ValueError):
            self.engine(User).filter(id='a').one()
        with self.assertRaises(DuplicateEntityException):
            self.engine(User).filter(id='a').one()

    def test_one_none(self):
        """ If no results, one() raises EntityNotFoundException """
        # For legacy reasons, make sure it also is a ValueError.
        with self.assertRaises(ValueError):
            self.engine(User).filter(id='a').one()
        with self.assertRaises(EntityNotFoundException):
            self.engine(User).filter(id='a').one()

    def test_count(self):
        """ Can return a count instead of the models """
        u = User(id='a', name='Adam')
        u2 = User(id='a', name='Aaron')
        self.engine.save([u, u2])
        count = self.engine(User).filter(id='a').count()
        self.assertEqual(count, 2)

    def test_iter(self):
        """ Queries can iterate over items """
        u = User(id='a', name='Adam')
        u2 = User(id='a', name='Aaron')
        self.engine.save([u, u2])

        users = [u, u2]
        for item in self.engine.query(User).filter(id='a'):
            self.assertTrue(item in users)
            users.remove(item)

    def test_force_hash_key(self):
        """ Queries must specify hash key """
        u = User(id='a', name='Adam')
        self.engine.save(u)
        with self.assertRaises(ValueError):
            self.engine.query(User).all()

    def test_filter_hash_key(self):
        """ Queries can filter by hash key """
        u = User(id='a', name='Adam')
        u2 = User(id='b', name='Billy')
        self.engine.save([u, u2])

        results = self.engine.query(User).filter(User.id == 'a').all()
        self.assertEquals(results, [u])

    def test_limit(self):
        """ Queries can have a limit """
        u = User(id='a', name='Adam')
        u2 = User(id='a', name='Aaron')
        self.engine.save([u, u2])

        results = self.engine.query(User).filter(User.id == 'a').limit(1).all()
        self.assertEquals(len(results), 1)

    def test_delete(self):
        """ Queries can selectively delete items """
        u = User(id='a', name='Adam')
        u2 = User(id='b', name='Billy')
        self.engine.save([u, u2])

        count = self.engine.query(User).filter(User.id == 'a').delete()
        self.assertEquals(count, 1)
        results = self.engine.scan(User).all()
        self.assertEquals(results, [u2])

    def test_filter_chain(self):
        """ Queries can chain filters """
        u = User(id='a', name='Adam')
        u2 = User(id='b', name='Billy')
        self.engine.save([u, u2])

        results = self.engine.query(User).filter(User.id == 'a')\
            .filter(User.name == 'Adam').all()
        self.assertEquals(results, [u])

    def test_filter_and(self):
        """ Queries can and filters together """
        u = User(id='a', name='Adam')
        u2 = User(id='b', name='Billy')
        self.engine.save([u, u2])

        results = self.engine.query(User).filter((User.id == 'a') &
                                                 (User.name == 'Adam')).all()
        self.assertEquals(results, [u])

    def test_filter_lt(self):
        """ Queries can filter lt """
        u = User(id='a', name='Adam')
        u2 = User(id='a', name='Aaron')
        self.engine.save([u, u2])

        results = self.engine.query(User).filter(User.id == 'a')\
            .filter(User.name < 'Adam').all()
        self.assertEquals(results, [u2])

    def test_filter_lte(self):
        """ Queries can filter lte """
        u = User(id='a', name='Aaron')
        u2 = User(id='a', name='Adam')
        u3 = User(id='a', name='Alison')
        self.engine.save([u, u2, u3])

        results = self.engine.query(User).filter(User.id == 'a')\
            .filter(User.name <= u2.name).all()
        self.assertEquals(len(results), 2)
        self.assertTrue(u in results)
        self.assertTrue(u2 in results)

    def test_filter_gt(self):
        """ Queries can filter gt """
        u = User(id='a', name='Adam')
        u2 = User(id='a', name='Aaron')
        self.engine.save([u, u2])

        results = self.engine.query(User).filter(User.id == 'a')\
            .filter(User.name > 'Aaron').all()
        self.assertEquals(results, [u])

    def test_filter_gte(self):
        """ Queries can filter gte """
        u = User(id='a', name='Aaron')
        u2 = User(id='a', name='Adam')
        u3 = User(id='a', name='Alison')
        self.engine.save([u, u2, u3])

        results = self.engine.query(User).filter(User.id == 'a')\
            .filter(User.name >= u2.name).all()
        self.assertEquals(len(results), 2)
        self.assertTrue(u2 in results)
        self.assertTrue(u3 in results)

    def test_filter_between(self):
        """ Queries can filter between """
        u = User(id='a', name='Aaron', score=1)
        u2 = User(id='a', name='Adam', score=2)
        u3 = User(id='a', name='Alison', score=3)
        u4 = User(id='a', name='Andrew', score=4)
        self.engine.save([u, u2, u3, u4])

        results = self.engine.query(User).filter(User.id == 'a')\
            .filter(User.score.between_(u2.score, u3.score)).all()
        self.assertEquals(len(results), 2)
        self.assertTrue(u2 in results)
        self.assertTrue(u3 in results)

    def test_filter_beginswith(self):
        """ Queries can filter beginswith """
        u = User(id='a', name='Adam')
        u2 = User(id='a', name='Aaron')
        self.engine.save([u, u2])

        results = self.engine.query(User).filter(User.id == 'a')\
            .filter(User.name.beginswith_('Ad')).all()
        self.assertEquals(results, [u])

    def test_filter_ne(self):
        """ Queries can filter ne """
        u = User(id='a', name='Adam')
        u2 = User(id='a', name='Aaron')
        self.engine.save([u, u2])

        ret = self.engine.query(User).filter(User.id == 'a')\
            .filter(User.name != 'Adam').one()
        self.assertEqual(ret, u2)

    def test_filter_in(self):
        """ Queries can filter in """
        u = User(id='a', name='Adam')
        u2 = User(id='a', name='Aaron')
        self.engine.save([u, u2])

        ret = self.engine.query(User).filter(User.id == 'a')\
            .filter(User.name.in_(set(['Adam']))).one()
        self.assertEqual(ret, u)

    def test_filter_contains(self):
        """ Queries can filter contains """
        u = User(id='a', name='Adam', str_set=set(['foo', 'bar']))
        u2 = User(id='a', name='Aaron', str_set=set(['bar']))
        self.engine.save([u, u2])

        ret = self.engine.query(User).filter(User.id == 'a')\
            .filter(User.str_set.contains_('foo')).one()
        self.assertEqual(ret, u)

    def test_filter_null(self):
        """ Queries can filter null """
        u = User(id='a', name='Adam', str_set=set(['foo']))
        u2 = User(id='a', name='Aaron')
        self.engine.save([u, u2])

        ret = self.engine.query(User).filter(User.id == 'a')\
            .filter(User.str_set == None).one()  # noqa
        self.assertEqual(ret, u2)

    def test_filter_not_null(self):
        """ Queries can filter not null """
        u = User(id='a', name='Adam', str_set=set(['foo']))
        u2 = User(id='a', name='Aaron')
        self.engine.save([u, u2])

        ret = self.engine.query(User).filter(User.id == 'a')\
            .filter(User.str_set != None).one()  # noqa
        self.assertEqual(ret, u)

    def test_smart_local_index(self):
        """ Queries auto-select local secondary index """
        u = User(id='a', name='Adam', score=50)
        u2 = User(id='a', name='Aaron', score=100)
        self.engine.save([u, u2])

        results = self.engine.query(User).filter(User.id == 'a')\
            .filter(User.score > 75).all()
        self.assertEquals(results, [u2])

    def test_smart_global_index(self):
        """ Queries auto-select global secondary index """
        u = User(id='a', name='Adam', score=50)
        u2 = User(id='b', name='Adam', score=100)
        self.engine.save([u, u2])

        results = self.engine.query(User).filter(User.name == 'Adam')\
            .filter(User.score > 75).all()
        self.assertEquals(results, [u2])

    def test_double_limit(self):
        """ Calling limit twice on the same query raises error """
        with self.assertRaises(ValueError):
            self.engine.query(User).filter(name='Adam').limit(10).limit(5).all()

    def test_double_index(self):
        """ Calling index twice on the same query raises error """
        with self.assertRaises(ValueError):
            self.engine.query(User).filter(name='Adam').index('name-index')\
                .index('score-index').all()

    def test_filter_non_indexed(self):
        """ Queries can filter non-indexed fields """
        u = User(id='a', name='Adam', bio='bar')
        u2 = User(id='a', name='Billy', bio='baz')
        self.engine.save([u, u2])

        results = self.engine.query(User).filter(User.id == 'a')\
            .filter(bio='bar').all()
        self.assertEquals(results, [u])

    def test_filter_or(self):
        """ Queries can join filter constraints with OR """
        u = User(id='a', name='Adam', bio='bar')
        u2 = User(id='a', name='Billy', plan='baz')
        u3 = User(id='a', name='Celine', bio='not', plan='this')
        self.engine.save([u, u2, u3])

        results = self.engine.query(User).filter(User.id == 'a')\
            .filter(bio='bar', plan='baz').all(filter_or=True)
        self.assertEqual(len(results), 2)
        self.assertTrue(u in results)
        self.assertTrue(u2 in results)

    def test_filter_inequality(self):
        """ Queries can use inequality filters on non-indexed fields """
        u = User(id='a', name='Adam', bio='aaa')
        u2 = User(id='a', name='Billy', bio='zzz')
        self.engine.save([u, u2])

        results = self.engine.query(User).filter(User.id == 'a')\
            .filter(User.bio < 'ddd').all()
        self.assertEquals(results, [u])


class TestCompositeQueries(DynamoSystemTest):

    """ Tests for table queries """
    models = [Post]

    def test_composite_query(self):
        """ Can query composite fields """
        p = Post(type='tweet', id='1234')
        self.engine.save(p)

        results = self.engine(Post).filter(uid='tweet:1234').all()
        self.assertEquals(results, [p])

    def test_composite_query_piecewise(self):
        """ Can query composite fields by individual pieces """
        p = Post(type='tweet', id='1234')
        self.engine.save(p)

        results = self.engine(Post).filter(type='tweet', id='1234').all()
        self.assertEquals(results, [p])

    def test_composite_local_index(self):
        """ Auto-select composite local secondary indexes """
        p = Post(type='tweet', id='1234')
        self.engine.save(p)

        results = self.engine(Post).filter(type='tweet', id='1234',
                                           score=0).all()
        self.assertEquals(results, [p])

    def test_composite_local_index_piecewise(self):
        """ Auto-select composite local secondary indexes by pieces """
        p = Post(type='tweet', id='1234')
        self.engine.save(p)

        results = self.engine(Post).filter(type='tweet', id='1234', ts=0,
                                           upvotes=0).all()
        self.assertEquals(results, [p])

    def test_composite_global_index(self):
        """ Auto-select composite global secondary indexes """
        p = Post(type='tweet', id='1234', username='abc')
        self.engine.save(p)

        results = self.engine(Post).filter(username='abc', score=0) \
            .index('name-index').all()
        self.assertEquals(results, [p])

    def test_composite_global_index_piecewise(self):
        """ Use composite global secondary indexes by pieces """
        p = Post(type='tweet', id='1234', username='abc')
        self.engine.save(p)

        results = self.engine(Post).filter(username='abc', ts=0,
                                           upvotes=0).index('name-index').all()
        self.assertEquals(results, [p])

    def test_ambiguous_index(self):
        """ Error raised if index name is ambiguous """
        p = Post(type='tweet', id='1234', username='abc')
        self.engine.save(p)

        with self.assertRaises(ValueError):
            self.engine(Post).filter(username='abc').all()

    def test_select_index(self):
        """ Index name can be specified """
        p = Post(type='tweet', id='1234', username='abc')
        self.engine.save(p)

        results = self.engine(Post).filter(username='abc')\
            .index('name-index').all()
        self.assertEquals(results, [p])

    def test_no_index(self):
        """ If no index is found, error is raised """
        p = Post(type='tweet', id='1234')
        self.engine.save(p)

        with self.assertRaises(ValueError):
            self.engine(Post).filter(Post.username == 'a')\
                .filter(Post.upvotes == 4).all()

    def test_no_range(self):
        """ Can query on an index even if there is no range key """
        p = Post(type='tweet', id='1234', username='abc')
        self.engine.save(p)

        ret = self.engine.query(Post).filter(id='1234', type='tweet',
                                             username='abc').all()
        self.assertEqual(ret, [p])


class Widget(Model):

    """ Test model for ordering """
    id = Field(hash_key=True)
    name = Field(range_key=True)
    alpha = Field(data_type=int, index='alpha-index')
    beta = Field(data_type=int, index='beta-index')


class TestOrder(DynamoSystemTest):

    """ Test results ordering """
    models = [Widget]

    def _add_widgets(self):
        """ Add a bunch of widgets with different alpha/beta values """
        for i in _xrange(10):
            w = Widget('a', str(i), alpha=i)
            w.beta = (i + 5) % 10
            self.engine.save(w)

    def test_default_acending(self):
        """ By default results are in ascending order """
        self._add_widgets()
        items = self.engine(Widget).filter(id='a').index('alpha-index').all()
        alpha = [item.alpha for item in items]
        self.assertEquals(alpha, sorted(alpha))

    def test_desc(self):
        """ desc=True orders returns items in descending order """
        self._add_widgets()
        items = self.engine(Widget).filter(id='a')\
            .index('alpha-index').all(desc=True)
        alpha = [item.alpha for item in items]
        alpha.reverse()
        self.assertEquals(alpha, sorted(alpha))

    def test_order_index(self):
        """ Results are ordered by the index specified """
        self._add_widgets()
        items = self.engine(Widget).filter(id='a').index('beta-index').all()
        beta = [item.beta for item in items]
        self.assertEquals(beta, sorted(beta))


class SingleKeyModel(Model):

    """ Model with a no range key """
    id = Field(hash_key=True)

    def __init__(self, id='a'):
        super(SingleKeyModel, self).__init__(id)


class TestEngine(DynamoSystemTest):

    """ Tests for misc engine functionality """
    models = [Post, SingleKeyModel]

    def test_get(self):
        """ Fetch item directly by primary key """
        p = Post(type='tweet', id='1234')
        self.engine.save(p)

        ret = self.engine.get(Post, uid='tweet:1234', score=0)
        self.assertEqual(ret, p)

    def test_get_many(self):
        """ Fetch multiple items directly by primary key """
        p = Post(type='tweet', id='1234')
        p2 = Post(type='post', id='2345')
        self.engine.save([p, p2])

        ret = self.engine.get(Post,
                              [{'uid': 'tweet:1234', 'score': 0},
                               {'uid': 'post:2345', 'score': 0}])
        self.assertEqual(len(ret), 2)
        self.assertTrue(p in ret)
        self.assertTrue(p2 in ret)

    def test_query_no_range(self):
        """ Can query a model that has no range key """
        m = SingleKeyModel()
        self.engine.save(m)
        ret = self.engine(SingleKeyModel).filter(id='a').all()
        self.assertEqual(ret, [m])

    def test_get_composite_pieces(self):
        """ Fetch item directly by pieces of composite primary key """
        p = Post(type='tweet', id='1234')
        self.engine.save(p)

        ret = self.engine.get(Post, type='tweet', id='1234', ts=0, upvotes=0)
        self.assertEqual(ret, p)

    def test_get_empty(self):
        """ Fetching empty list of keys returns empty list """
        ret = self.engine.get(Post, [])
        self.assertEqual(ret, [])

    def test_get_missing(self):
        """ Fetching a missing item returns None """
        ret = self.engine.get(Post, uid='a', score=4)
        self.assertIsNone(ret)

    def test_get_smart_scope(self):
        """ Models with no range key can fetch from string """
        m = SingleKeyModel()
        self.engine.save(m)

        ret = self.engine.get(SingleKeyModel, [m.id])
        self.assertEqual(ret, [m])

    def test_delete_key(self):
        """ Delete item directly by primary key """
        m = SingleKeyModel()
        self.engine.save(m)

        self.engine.delete_key(SingleKeyModel, id=m.id)
        self.assertIsNone(self.engine.scan(SingleKeyModel).first())

    def test_delete_key_many(self):
        """ Delete multiple keys directly by primary key """

    def test_delete_key_composite_pieces(self):
        """ Delete item directly by pieces of composite primary key """

    def test_delete_smart_scope(self):
        """ Models with no range key can delete from string """
        m = SingleKeyModel()
        self.engine.save(m)

        self.engine.delete_key(SingleKeyModel, [m.id])
        self.assertIsNone(self.engine.scan(SingleKeyModel).first())

    def test_delete_key_empty(self):
        """ No error if deleting no keys """
        self.engine.delete_key(SingleKeyModel, [])
