""" Tests for table scans """
from . import BaseSystemTest
from .test_queries import User


class TestScan(BaseSystemTest):

    """ Tests for table scans """
    models = [User]

    def test_all(self):
        """ Scan returns all elements in a table """
        u = User(id='a', name='Adam')
        u2 = User(id='b', name='Billy')
        self.engine.save([u, u2])

        results = self.engine.scan(User).all()
        self.assertEquals(len(results), 2)
        self.assertTrue(u in results)
        self.assertTrue(u2 in results)

    def test_limit(self):
        """ Scan can have a limit """
        u = User(id='a', name='Adam')
        u2 = User(id='a', name='Aaron')
        self.engine.save([u, u2])

        results = self.engine.scan(User).limit(1).all()
        self.assertEquals(len(results), 1)

    def test_delete(self):
        """ Scan can selectively delete items """
        u = User(id='a', name='Adam')
        u2 = User(id='b', name='Billy')
        self.engine.save([u, u2])

        count = self.engine.scan(User).filter(User.name == 'Adam').delete()
        self.assertEquals(count, 1)
        results = self.engine.scan(User).all()
        self.assertEquals(results, [u2])

    def test_filter_eq(self):
        """ Scan can filter eq """
        u = User(id='a', name='Adam')
        u2 = User(id='b', name='Billy')
        self.engine.save([u, u2])

        results = self.engine.scan(User).filter(User.name == 'Adam').all()
        self.assertEquals(results, [u])

    def test_filter_lt(self):
        """ Scan can filter lt """
        u = User(id='a', name='Adam')
        u2 = User(id='a', name='Aaron')
        self.engine.save([u, u2])

        results = self.engine.scan(User).filter(User.name < 'Adam').all()
        self.assertEquals(results, [u2])

    def test_filter_lte(self):
        """ Scan can filter lte """
        u = User(id='a', name='Aaron')
        u2 = User(id='a', name='Adam')
        u3 = User(id='a', name='Alison')
        self.engine.save([u, u2, u3])

        results = self.engine.scan(User).filter(User.name <= u2.name).all()
        self.assertEquals(len(results), 2)
        self.assertTrue(u in results)
        self.assertTrue(u2 in results)

    def test_filter_gt(self):
        """ Scan can filter gt """
        u = User(id='a', name='Adam')
        u2 = User(id='a', name='Aaron')
        self.engine.save([u, u2])

        results = self.engine.scan(User).filter(User.name > 'Aaron').all()
        self.assertEquals(results, [u])

    def test_filter_gte(self):
        """ Scan can filter gte """
        u = User(id='a', name='Aaron')
        u2 = User(id='a', name='Adam')
        u3 = User(id='a', name='Alison')
        self.engine.save([u, u2, u3])

        results = self.engine.scan(User).filter(User.name >= u2.name).all()
        self.assertEquals(len(results), 2)
        self.assertTrue(u2 in results)
        self.assertTrue(u3 in results)

    def test_filter_beginswith(self):
        """ Scan can filter beginswith """
        u = User(id='a', name='Adam')
        u2 = User(id='a', name='Aaron')
        self.engine.save([u, u2])

        results = self.engine.scan(User)\
            .filter(User.name.beginswith_('Ad')).all()
        self.assertEquals(results, [u])

    def test_filter_ne(self):
        """ Scan can filter ne """
        u = User(id='a', name='Adam')
        u2 = User(id='a', name='Aaron')
        self.engine.save([u, u2])

        results = self.engine.scan(User).filter(User.name != 'Adam').all()
        self.assertEquals(results, [u2])

    def test_filter_in(self):
        """ Scan can filter in """
        u = User(id='a', name='Adam')
        u2 = User(id='a', name='Aaron')
        self.engine.save([u, u2])
        names = set([u.name])

        results = self.engine.scan(User).filter(User.name.in_(names)).all()
        self.assertEquals(results, [u])

    def test_filter_contains(self):
        """ Scan can filter contains """
        u = User(id='a', name='Adam', str_set=set(['hi']))
        u2 = User(id='a', name='Aaron')
        self.engine.save([u, u2])

        results = self.engine.scan(User)\
            .filter(User.str_set.contains_('hi')).all()
        self.assertEquals(results, [u])


class TestScanFilterOverflow(BaseSystemTest):

    """ Filter tests on overflow fields """
    models = [User]

    def test_filter_eq(self):
        """ Scan overflow field can filter eq """
        u = User(id='a', name='Adam', foobar='foo')
        u2 = User(id='b', name='Billy', foobar='bar')
        self.engine.save([u, u2])

        results = self.engine.scan(User)\
            .filter(User.field_('foobar') == 'foo').all()
        self.assertEquals(results, [u])

    def test_filter_lt(self):
        """ Scan overflow field can filter lt """
        u = User(id='a', name='Adam', foobar=5)
        u2 = User(id='a', name='Aaron', foobar=2)
        self.engine.save([u, u2])

        results = self.engine.scan(User).filter(
            User.field_('foobar') < 5).all()
        self.assertEquals(results, [u2])

    def test_filter_lte(self):
        """ Scan overflow field can filter lte """
        u = User(id='a', name='Aaron', foobar=1)
        u2 = User(id='a', name='Adam', foobar=2)
        u3 = User(id='a', name='Alison', foobar=3)
        self.engine.save([u, u2, u3])

        results = self.engine.scan(User)\
            .filter(User.field_('foobar') <= 2).all()
        self.assertEquals(len(results), 2)
        self.assertTrue(u in results)
        self.assertTrue(u2 in results)

    def test_filter_gt(self):
        """ Scan overflow field can filter gt """
        u = User(id='a', name='Adam', foobar=5)
        u2 = User(id='a', name='Aaron', foobar=2)
        self.engine.save([u, u2])

        results = self.engine.scan(User).filter(
            User.field_('foobar') > 2).all()
        self.assertEquals(results, [u])

    def test_filter_gte(self):
        """ Scan overflow field can filter gte """
        u = User(id='a', name='Aaron', foobar=1)
        u2 = User(id='a', name='Adam', foobar=2)
        u3 = User(id='a', name='Alison', foobar=3)
        self.engine.save([u, u2, u3])

        results = self.engine.scan(User)\
            .filter(User.field_('foobar') >= 2).all()
        self.assertEquals(len(results), 2)
        self.assertTrue(u2 in results)
        self.assertTrue(u3 in results)

    def test_filter_beginswith(self):
        """ Scan overflow field can filter beginswith """
        u = User(id='a', name='Adam', foobar="abc")
        u2 = User(id='a', name='Aaron', foobar="def")
        self.engine.save([u, u2])

        results = self.engine.scan(User)\
            .filter(User.field_('foobar').beginswith_('a')).all()
        self.assertEquals(results, [u])

    def test_filter_ne(self):
        """ Scan overflow field can filter ne """
        u = User(id='a', name='Adam', foobar='hi')
        u2 = User(id='a', name='Aaron', foobar='ih')
        self.engine.save([u, u2])

        results = self.engine.scan(User)\
            .filter(User.field_('foobar') != 'hi').all()
        self.assertEquals(results, [u2])

    def test_filter_in(self):
        """ Scan overflow field can filter in """
        u = User(id='a', name='Adam', foobar='hi')
        u2 = User(id='a', name='Aaron', foobar='ih')
        self.engine.save([u, u2])
        bars = set([u.foobar])

        results = self.engine.scan(User)\
            .filter(User.field_('foobar').in_(bars)).all()
        self.assertEquals(results, [u])

    def test_filter_contains(self):
        """ Scan overflow field can filter contains """
        u = User(id='a', name='Adam', foobar=set(['hi']))
        u2 = User(id='a', name='Aaron', foobar=set(['ih']))
        self.engine.save([u, u2])

        results = self.engine.scan(User)\
            .filter(User.field_('foobar').contains_('hi')).all()
        self.assertEquals(results, [u])

    def test_filter_null(self):
        """ Scan overflow field can filter null """
        u = User(id='a', name='Adam', foobar='hi')
        u2 = User(id='a', name='Aaron')
        self.engine.save([u, u2])

        results = self.engine.scan(User)\
            .filter(User.field_('foobar') == None).all()  # noqa
        self.assertEquals(results, [u2])

    def test_filter_not_null(self):
        """ Scan overflow field can filter not null """
        u = User(id='a', name='Adam', foobar='hi')
        u2 = User(id='a', name='Aaron')
        self.engine.save([u, u2])

        results = self.engine.scan(User)\
            .filter(User.field_('foobar') != None).all()  # noqa
        self.assertEquals(results, [u])
