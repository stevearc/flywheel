""" Tests for table scans """
from datetime import datetime, date, timedelta

from decimal import Decimal

from . import BaseSystemTest
from .test_queries import User
from flywheel import Model, Field


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


class Widget(Model):

    """ Test model with every data type """
    id = Field(hash_key=True)
    school = Field(data_type=unicode)
    count = Field(data_type=int)
    score = Field(data_type=float)
    isawesome = Field(data_type=bool)
    name = Field(data_type=str)
    friends = Field(data_type=set)
    data = Field(data_type=dict)
    queue = Field(data_type=list)
    created = Field(data_type=datetime)
    birthday = Field(data_type=date)
    price = Field(data_type=Decimal)

    def __init__(self, **kwargs):
        self.id = 'abc'
        for key, val in kwargs.iteritems():
            setattr(self, key, val)


class TestFilterFields(BaseSystemTest):

    """ Test query filters for every data type """
    models = [Widget]

    # UNICODE

    def test_eq_unicode(self):
        """ Can use equality filter on unicode fields """
        w = Widget()
        self.engine.save(w)
        ret = self.engine.scan(Widget).filter(Widget.id == w.id).first()
        self.assertEquals(w, ret)

    def test_ineq_unicode(self):
        """ Can use inequality filters on unicode fields """
        w = Widget(school='Harvard')
        self.engine.save(w)
        ret = self.engine.scan(Widget).filter(Widget.school < 'MIT').first()
        self.assertEquals(w, ret)

    def test_in_unicode(self):
        """ Can use 'in' filter on unicode fields """
        w = Widget()
        self.engine.save(w)
        ret = self.engine.scan(Widget).filter(Widget.id.in_([w.id])).first()
        self.assertEquals(w, ret)

    def test_beginswith_unicode(self):
        """ Can use 'beginswith' filter on unicode fields """
        w = Widget(id='abc')
        self.engine.save(w)
        ret = self.engine.scan(Widget)\
            .filter(Widget.id.beginswith_('a')).first()
        self.assertEquals(w, ret)

    def test_between_unicode(self):
        """ Can use 'between' filter on unicode fields """
        w = Widget(school='Harvard')
        self.engine.save(w)
        ret = self.engine.scan(Widget)\
            .filter(Widget.school.between_('Cornell', 'MIT')).first()
        self.assertEquals(w, ret)

    def test_contains_unicode(self):
        """ Cannot use 'contains' filter on unicode fields """
        w = Widget()
        self.engine.save(w)
        with self.assertRaises(TypeError):
            self.engine.scan(Widget).filter(Widget.id.contains_(w.id)).all()

    def test_ncontains_unicode(self):
        """ Cannot use 'ncontains' filter on unicode fields """
        w = Widget()
        self.engine.save(w)
        with self.assertRaises(TypeError):
            self.engine.scan(Widget).filter(Widget.id.ncontains_(w.id)).all()

    # STR

    def test_eq_str(self):
        """ Can use equality filter on str fields """
        w = Widget(name='dsa')
        self.engine.save(w)
        ret = self.engine.scan(Widget).filter(Widget.name == 'dsa').first()
        self.assertEquals(w, ret)

    def test_ineq_str(self):
        """ Can use inequality filters on str fields """
        w = Widget(name='dsa')
        self.engine.save(w)
        ret = self.engine.scan(Widget).filter(Widget.name > 'abc').first()
        self.assertEquals(w, ret)

    def test_in_str(self):
        """ Can use 'in' filter on str fields """
        w = Widget(name='dsa')
        self.engine.save(w)
        ret = self.engine.scan(Widget).filter(Widget.name.in_(['dsa'])).first()
        self.assertEquals(w, ret)

    def test_beginswith_str(self):
        """ Can use 'beginswith' filter on str fields """
        w = Widget(name='dsa')
        self.engine.save(w)
        ret = self.engine.scan(Widget)\
            .filter(Widget.name.beginswith_('d')).first()
        self.assertEquals(w, ret)

    def test_between_str(self):
        """ Can use 'between' filter on str fields """
        w = Widget(name='dsa')
        self.engine.save(w)
        ret = self.engine.scan(Widget)\
            .filter(Widget.name.between_('adam', 'rachel')).first()
        self.assertEquals(w, ret)

    def test_contains_str(self):
        """ Cannot use 'contains' filter on str fields """
        w = Widget(name='dsa')
        self.engine.save(w)
        with self.assertRaises(TypeError):
            self.engine.scan(Widget)\
                .filter(Widget.name.contains_('dsa')).all()

    def test_ncontains_str(self):
        """ Cannot use 'ncontains' filter on str fields """
        w = Widget(name='dsa')
        self.engine.save(w)
        with self.assertRaises(TypeError):
            self.engine.scan(Widget)\
                .filter(Widget.name.ncontains_('dsa')).all()

    # INT

    def test_eq_int(self):
        """ Can use equality filter on int fields """
        w = Widget(count=4)
        self.engine.save(w)
        ret = self.engine.scan(Widget).filter(Widget.count == 4).first()
        self.assertEquals(w, ret)

    def test_ineq_int(self):
        """ Can use inequality filters on int fields """
        w = Widget(count=4)
        self.engine.save(w)
        ret = self.engine.scan(Widget).filter(Widget.count >= 4).first()
        self.assertEquals(w, ret)

    def test_in_int(self):
        """ Can use 'in' filter on int fields """
        w = Widget(count=4)
        self.engine.save(w)
        ret = self.engine.scan(Widget).filter(Widget.count.in_([4])).first()
        self.assertEquals(w, ret)

    def test_beginswith_int(self):
        """ Cannot use 'beginswith' filter on int fields """
        w = Widget(count=4)
        self.engine.save(w)
        with self.assertRaises(TypeError):
            self.engine.scan(Widget).filter(Widget.count.beginswith_(4)).all()

    def test_between_int(self):
        """ Can use 'between' filter on int fields """
        w = Widget(count=5)
        self.engine.save(w)
        ret = self.engine.scan(Widget)\
            .filter(Widget.count.between_(4, 6)).first()
        self.assertEquals(w, ret)

    def test_contains_int(self):
        """ Cannot use 'contains' filter on int fields """
        w = Widget(count=4)
        self.engine.save(w)
        with self.assertRaises(TypeError):
            self.engine.scan(Widget).filter(Widget.count.contains_(4)).all()

    def test_ncontains_int(self):
        """ Cannot use 'ncontains' filter on int fields """
        w = Widget(count=4)
        self.engine.save(w)
        with self.assertRaises(TypeError):
            self.engine.scan(Widget).filter(Widget.count.ncontains_(4)).all()

    # FLOAT

    def test_eq_float(self):
        """ Can use equality filter on float fields """
        w = Widget(score=1.3)
        self.engine.save(w)
        ret = self.engine.scan(Widget).filter(Widget.score == 1.3).first()
        self.assertEquals(w, ret)

    def test_ineq_float(self):
        """ Can use inequality filters on float fields """
        w = Widget(score=1.3)
        self.engine.save(w)
        ret = self.engine.scan(Widget).filter(Widget.score < 2.3).first()
        self.assertEquals(w, ret)

    def test_in_float(self):
        """ Can use 'in' filter on float fields """
        w = Widget(score=1.3)
        self.engine.save(w)
        ret = self.engine.scan(Widget).filter(Widget.score.in_([1.3])).first()
        self.assertEquals(w, ret)

    def test_beginswith_float(self):
        """ Cannot use 'beginswith' filter on float fields """
        w = Widget(score=1.3)
        self.engine.save(w)
        with self.assertRaises(TypeError):
            self.engine.scan(Widget)\
                .filter(Widget.score.beginswith_(1.3)).all()

    def test_between_float(self):
        """ Can use 'between' filter on float fields """
        w = Widget(score=4.4)
        self.engine.save(w)
        ret = self.engine.scan(Widget)\
            .filter(Widget.score.between_(4.3, 5.6)).first()
        self.assertEquals(w, ret)

    def test_contains_float(self):
        """ Cannot use 'contains' filter on float fields """
        w = Widget(score=1.3)
        self.engine.save(w)
        with self.assertRaises(TypeError):
            self.engine.scan(Widget)\
                .filter(Widget.score.contains_(1.3)).all()

    def test_ncontains_float(self):
        """ Cannot use 'ncontains' filter on float fields """
        w = Widget(score=1.3)
        self.engine.save(w)
        with self.assertRaises(TypeError):
            self.engine.scan(Widget)\
                .filter(Widget.score.ncontains_(1.3)).all()

    # DECIMAL

    def test_eq_decimal(self):
        """ Can use equality filter on decimal fields """
        w = Widget(price=Decimal('3.50'))
        self.engine.save(w)
        ret = self.engine.scan(Widget).filter(Widget.price ==
                                              Decimal('3.50')).first()
        self.assertEquals(w, ret)

    def test_ineq_decimal(self):
        """ Can use inequality filters on decimal fields """
        w = Widget(price=Decimal('3.50'))
        self.engine.save(w)
        ret = self.engine.scan(Widget).filter(Widget.price > 2.3).first()
        self.assertEquals(w, ret)

    def test_in_decimal(self):
        """ Can use 'in' filter on decimal fields """
        w = Widget(price=Decimal('3.50'))
        self.engine.save(w)
        ret = self.engine.scan(Widget)\
            .filter(Widget.price.in_([Decimal('3.50')])).first()
        self.assertEquals(w, ret)

    def test_beginswith_decimal(self):
        """ Cannot use 'beginswith' filter on decimal fields """
        w = Widget(price=Decimal('3.50'))
        self.engine.save(w)
        with self.assertRaises(TypeError):
            self.engine.scan(Widget)\
                .filter(Widget.price.beginswith_(Decimal('3.50'))).all()

    def test_between_decimal(self):
        """ Can use 'between' filter on decimal fields """
        w = Widget(price=Decimal('3.50'))
        self.engine.save(w)
        ret = self.engine.scan(Widget)\
            .filter(Widget.price.between_('3.40', '3.55')).first()
        self.assertEquals(w, ret)

    def test_contains_decimal(self):
        """ Cannot use 'contains' filter on decimal fields """
        w = Widget(price=Decimal('3.50'))
        self.engine.save(w)
        with self.assertRaises(TypeError):
            self.engine.scan(Widget)\
                .filter(Widget.price.contains_(Decimal('3.50'))).all()

    def test_ncontains_decimal(self):
        """ Cannot use 'ncontains' filter on decimal fields """
        w = Widget(price=Decimal('3.50'))
        self.engine.save(w)
        with self.assertRaises(TypeError):
            self.engine.scan(Widget)\
                .filter(Widget.price.ncontains_(Decimal('3.50'))).all()

    # BOOL

    def test_eq_bool(self):
        """ Can use equality filter on bool fields """
        w = Widget(isawesome=True)
        self.engine.save(w)
        ret = self.engine.scan(Widget)\
                .filter(Widget.isawesome == True).first()  # noqa
        self.assertEquals(w, ret)

    def test_ineq_bool(self):
        """ Cannot use inequality filters on bool fields """
        w = Widget(isawesome=True)
        self.engine.save(w)
        with self.assertRaises(TypeError):
            self.engine.scan(Widget).filter(Widget.isawesome < True).all()

    def test_in_bool(self):
        """ Cannot use 'in' filter on bool fields """
        w = Widget(isawesome=True)
        self.engine.save(w)
        with self.assertRaises(TypeError):
            self.engine.scan(Widget)\
                .filter(Widget.isawesome.in_([True])).all()

    def test_beginswith_bool(self):
        """ Cannot use 'beginswith' filter on bool fields """
        w = Widget(isawesome=True)
        self.engine.save(w)
        with self.assertRaises(TypeError):
            self.engine.scan(Widget)\
                .filter(Widget.isawesome.beginswith_('T')).all()

    def test_between_bool(self):
        """ Cannot use 'between' filter on bool fields """
        w = Widget(isawesome=True)
        self.engine.save(w)
        with self.assertRaises(TypeError):
            self.engine.scan(Widget)\
                .filter(Widget.isawesome.between_(0, 2)).all()

    def test_contains_bool(self):
        """ Cannot use 'contains' filter on bool fields """
        w = Widget(isawesome=True)
        self.engine.save(w)
        with self.assertRaises(TypeError):
            self.engine.scan(Widget)\
                .filter(Widget.isawesome.contains_(True)).all()

    def test_ncontains_bool(self):
        """ Cannot use 'ncontains' filter on bool fields """
        w = Widget(isawesome=True)
        self.engine.save(w)
        with self.assertRaises(TypeError):
            self.engine.scan(Widget)\
                .filter(Widget.isawesome.ncontains_(True)).all()

    # DATETIME

    def test_eq_datetime(self):
        """ Can use equality filter on datetime fields """
        n = datetime.utcnow()
        w = Widget(created=n)
        self.engine.save(w)
        ret = self.engine.scan(Widget).filter(Widget.created == n).first()
        self.assertEquals(w, ret)

    def test_ineq_datetime(self):
        """ Can use inequality filters on datetime fields """
        n = datetime.utcnow()
        later = n + timedelta(seconds=1)
        w = Widget(created=n)
        self.engine.save(w)
        ret = self.engine.scan(Widget).filter(Widget.created < later).first()
        self.assertEquals(w, ret)

    def test_in_datetime(self):
        """ Can use 'in' filter on datetime fields """
        n = datetime.utcnow()
        w = Widget(created=n)
        self.engine.save(w)
        ret = self.engine.scan(Widget).filter(Widget.created.in_([n])).first()
        self.assertEquals(w, ret)

    def test_beginswith_datetime(self):
        """ Cannot use 'beginswith' filter on datetime fields """
        n = datetime.utcnow()
        w = Widget(created=n)
        self.engine.save(w)
        with self.assertRaises(TypeError):
            self.engine.scan(Widget)\
                .filter(Widget.created.beginswith_(n)).all()

    def test_between_datetime(self):
        """ Can use 'between' filter on datetime fields """
        n = datetime.utcnow()
        w = Widget(created=n)
        self.engine.save(w)
        early, late = n - timedelta(seconds=5), n + timedelta(minutes=4)
        ret = self.engine.scan(Widget)\
            .filter(Widget.created.between_(early, late)).first()
        self.assertEquals(w, ret)

    def test_contains_datetime(self):
        """ Cannot use 'contains' filter on datetime fields """
        n = datetime.utcnow()
        w = Widget(created=n)
        self.engine.save(w)
        with self.assertRaises(TypeError):
            self.engine.scan(Widget).filter(Widget.created.contains_(n)).all()

    def test_ncontains_datetime(self):
        """ Cannot use 'ncontains' filter on datetime fields """
        n = datetime.utcnow()
        w = Widget(created=n)
        self.engine.save(w)
        with self.assertRaises(TypeError):
            self.engine.scan(Widget)\
                .filter(Widget.created.ncontains_(n)).all()

    # DATE

    def test_eq_date(self):
        """ Can use equality filter on date fields """
        n = date.today()
        w = Widget(birthday=n)
        self.engine.save(w)
        ret = self.engine.scan(Widget).filter(Widget.birthday == n).first()
        self.assertEquals(w, ret)

    def test_ineq_date(self):
        """ Can use inequality filters on date fields """
        n = date.today()
        later = n + timedelta(days=1)
        w = Widget(birthday=n)
        self.engine.save(w)
        ret = self.engine.scan(Widget).filter(Widget.birthday < later).first()
        self.assertEquals(w, ret)

    def test_in_date(self):
        """ Can use 'in' filter on date fields """
        n = date.today()
        w = Widget(birthday=n)
        self.engine.save(w)
        ret = self.engine.scan(Widget).filter(Widget.birthday.in_([n])).first()
        self.assertEquals(w, ret)

    def test_beginswith_date(self):
        """ Cannot use 'beginswith' filter on date fields """
        n = date.today()
        w = Widget(birthday=n)
        self.engine.save(w)
        with self.assertRaises(TypeError):
            self.engine.scan(Widget)\
                .filter(Widget.birthday.beginswith_(n)).all()

    def test_between_date(self):
        """ Can use 'between' filter on date fields """
        n = date.today()
        w = Widget(birthday=n)
        self.engine.save(w)
        early, late = n, n + timedelta(days=2)
        ret = self.engine.scan(Widget)\
            .filter(Widget.birthday.between_(early, late)).first()
        self.assertEquals(w, ret)

    def test_contains_date(self):
        """ Cannot use 'contains' filter on date fields """
        n = date.today()
        w = Widget(birthday=n)
        self.engine.save(w)
        with self.assertRaises(TypeError):
            self.engine.scan(Widget).filter(Widget.birthday.contains_(n)).all()

    def test_ncontains_date(self):
        """ Cannot use 'ncontains' filter on date fields """
        n = date.today()
        w = Widget(birthday=n)
        self.engine.save(w)
        with self.assertRaises(TypeError):
            self.engine.scan(Widget)\
                .filter(Widget.birthday.ncontains_(n)).all()

    # SET

    def test_eq_set(self):
        """ Cannot use equality filter on set fields """
        f = set(['a'])
        w = Widget(friends=f)
        self.engine.save(w)
        with self.assertRaises(TypeError):
            self.engine.scan(Widget).filter(Widget.friends == f).all()

    def test_ineq_set(self):
        """ Cannot use inequality filters on set fields """
        f = set(['a'])
        f2 = set(['a', 'b', 'c'])
        w = Widget(friends=f)
        self.engine.save(w)
        with self.assertRaises(TypeError):
            self.engine.scan(Widget).filter(Widget.friends < f2).all()

    def test_in_set(self):
        """ Cannot use 'in' filter on set fields """
        f = set(['a'])
        f2 = set(['a', 'b', 'c'])
        w = Widget(friends=f)
        self.engine.save(w)
        with self.assertRaises(TypeError):
            self.engine.scan(Widget).filter(Widget.friends.in_(f2)).all()

    def test_beginswith_set(self):
        """ Cannot use 'beginswith' filter on set fields """
        f = set(['a'])
        f2 = set(['a', 'b', 'c'])
        w = Widget(friends=f)
        self.engine.save(w)
        with self.assertRaises(TypeError):
            self.engine.scan(Widget)\
                .filter(Widget.friends.beginswith_(f2)).all()

    def test_between_set(self):
        """ Cannot use 'between' filter on set fields """
        f = set(['a'])
        f2 = set(['a', 'b', 'c'])
        w = Widget(friends=f)
        self.engine.save(w)
        with self.assertRaises(TypeError):
            self.engine.scan(Widget)\
                .filter(Widget.friends.between_(f, f2)).all()

    def test_contains_set(self):
        """ Can use 'contains' filter on set fields """
        f = set(['a'])
        w = Widget(friends=f)
        self.engine.save(w)
        ret = self.engine.scan(Widget)\
            .filter(Widget.friends.contains_('a')).first()
        self.assertEquals(ret, w)

    def test_ncontains_set(self):
        """ Can use 'ncontains' filter on set fields """
        f = set(['a'])
        w = Widget(friends=f)
        self.engine.save(w)
        ret = self.engine.scan(Widget)\
            .filter(Widget.friends.ncontains_('b')).first()
        self.assertEquals(ret, w)

    # DICT

    def test_eq_dict(self):
        """ Cannot use equality filter on dict fields """
        d = {'foo': 'bar'}
        w = Widget(data=d)
        self.engine.save(w)
        with self.assertRaises(TypeError):
            self.engine.scan(Widget).filter(Widget.data == d).all()

    def test_ineq_dict(self):
        """ Cannot use inequality filters on dict fields """
        d = {'foo': 'bar'}
        w = Widget(data=d)
        self.engine.save(w)
        with self.assertRaises(TypeError):
            self.engine.scan(Widget).filter(Widget.data < d).all()

    def test_in_dict(self):
        """ Cannot use 'in' filter on dict fields """
        d = {'foo': 'bar'}
        w = Widget(data=d)
        self.engine.save(w)
        with self.assertRaises(ValueError):
            self.engine.scan(Widget).filter(Widget.data.in_(d)).all()

    def test_beginswith_dict(self):
        """ Cannot use 'beginswith' filter on dict fields """
        d = {'foo': 'bar'}
        w = Widget(data=d)
        self.engine.save(w)
        with self.assertRaises(TypeError):
            self.engine.scan(Widget).filter(Widget.data.beginswith_(d)).all()

    def test_between_dict(self):
        """ Cannot use 'between' filter on dict fields """
        d = {'foo': 'bar'}
        w = Widget(data=d)
        self.engine.save(w)
        with self.assertRaises(TypeError):
            self.engine.scan(Widget).filter(Widget.data.between_(d, d)).all()

    def test_contains_dict(self):
        """ Cannot use 'contains' filter on dict fields """
        d = {'foo': 'bar'}
        w = Widget(data=d)
        self.engine.save(w)
        with self.assertRaises(TypeError):
            self.engine.scan(Widget).filter(Widget.data.contains_(d)).all()

    def test_ncontains_dict(self):
        """ Cannot use 'ncontains' filter on dict fields """
        d = {'foo': 'bar'}
        w = Widget(data=d)
        self.engine.save(w)
        with self.assertRaises(TypeError):
            self.engine.scan(Widget).filter(Widget.data.ncontains_(d)).all()

    # LIST

    def test_eq_list(self):
        """ Cannot use equality filter on list fields """
        q = ['a']
        w = Widget(queue=q)
        self.engine.save(w)
        with self.assertRaises(TypeError):
            self.engine.scan(Widget).filter(Widget.queue == q).all()

    def test_ineq_list(self):
        """ Cannot use inequality filters on list fields """
        q = ['a']
        w = Widget(queue=q)
        self.engine.save(w)
        with self.assertRaises(TypeError):
            self.engine.scan(Widget).filter(Widget.queue <= q).all()

    def test_in_list(self):
        """ Cannot use 'in' filter on list fields """
        q = ['a']
        w = Widget(queue=q)
        self.engine.save(w)
        with self.assertRaises(TypeError):
            self.engine.scan(Widget).filter(Widget.queue.in_([q])).all()

    def test_beginswith_list(self):
        """ Cannot use 'beginswith' filter on list fields """
        q = ['a']
        w = Widget(queue=q)
        self.engine.save(w)
        with self.assertRaises(TypeError):
            self.engine.scan(Widget).filter(Widget.queue.beginswith_(q)).all()

    def test_between_list(self):
        """ Cannot use 'between' filter on list fields """
        q = ['a']
        w = Widget(queue=q)
        self.engine.save(w)
        with self.assertRaises(TypeError):
            self.engine.scan(Widget).filter(Widget.queue.between_(q, q)).all()

    def test_contains_list(self):
        """ Cannot use 'contains' filter on list fields """
        q = ['a']
        w = Widget(queue=q)
        self.engine.save(w)
        with self.assertRaises(TypeError):
            self.engine.scan(Widget).filter(Widget.queue.contains_(q)).all()

    def test_ncontains_list(self):
        """ Cannot use 'ncontains' filter on list fields """
        q = ['a']
        w = Widget(queue=q)
        self.engine.save(w)
        with self.assertRaises(TypeError):
            self.engine.scan(Widget).filter(Widget.queue.ncontains_(q)).all()
