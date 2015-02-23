""" Tests for fields """
import six
import zlib
from datetime import datetime, date

import json
from decimal import Decimal
from flywheel.fields.types import DictType, register_type

from flywheel import (Field, Composite, Model, NUMBER, BINARY, STRING_SET,
                      NUMBER_SET, BINARY_SET, Binary, GlobalIndex, set_)
from flywheel.tests import DynamoSystemTest
from flywheel.fields.types import UTC


try:
    import unittest2 as unittest  # pylint: disable=F0401
except ImportError:
    import unittest

# pylint: disable=E1101


class CompressedDict(DictType):

    """ Custom field type that compresses data """
    data_type = 'zdict'
    ddb_data_type = BINARY

    def ddb_dump(self, value):
        dumped = json.dumps(value)
        if isinstance(dumped, six.text_type):
            dumped = dumped.encode('ascii')
        return Binary(zlib.compress(dumped))

    def ddb_load(self, value):
        decompressed = zlib.decompress(value.value)
        if isinstance(decompressed, six.binary_type):
            decompressed = decompressed.decode('ascii')
        return json.loads(decompressed)


register_type(CompressedDict)


class Widget(Model):

    """ Model for testing default field values """
    __metadata__ = {
        'global_indexes': [
            GlobalIndex('gindex', 'string2', 'num'),
        ],
    }
    string = Field(hash_key=True)
    string2 = Field()
    num = Field(data_type=NUMBER)
    binary = Field(data_type=BINARY, coerce=True)
    str_set = Field(data_type=STRING_SET)
    num_set = Field(data_type=NUMBER_SET)
    bin_set = Field(data_type=BINARY_SET)
    data_dict = Field(data_type=dict)
    data_list = Field(data_type=list)
    bigdata = Field(data_type=CompressedDict)
    natural_num = Field(data_type=int, check=lambda x: x >= 0, default=1)
    check_num = Field(data_type=int,
                      check=(lambda x: x != 0, lambda x: x != 2))
    not_null = Field(data_type=int, nullable=False, default=0)
    not_null_natural = Field(data_type=int, check=lambda x: x != 1,
                             nullable=False, default=0)

    def __init__(self, **kwargs):
        kwargs.setdefault('string', 'abc')
        super(Widget, self).__init__(**kwargs)


class TestCreateFields(unittest.TestCase):

    """ Tests related to the creation of Fields """

    def test_hash_and_range(self):
        """ A field cannot be both a hash_key and range_key """
        with self.assertRaises(ValueError):
            Field(hash_key=True, range_key=True)

    def test_unknown_data_type(self):
        """ Unknown data types are disallowed by Field """
        with self.assertRaises(TypeError):
            Field(data_type='flkask')

    def test_double_index(self):
        """ Field cannot be indexed twice """
        with self.assertRaises(ValueError):
            Field(index='ts-index').all_index('name-index')

    def test_index_hash_key(self):
        """ Cannot index the hash key """
        with self.assertRaises(ValueError):
            Field(hash_key=True, index='h-index')

    def test_index_range_key(self):
        """ Cannot index the range key """
        with self.assertRaises(ValueError):
            Field(range_key=True, index='r-index')

    def test_create_custom_type(self):
        """ Can create a field with a custom data type """
        Field(data_type='zdict')
        Field(data_type=CompressedDict)
        Field(data_type=CompressedDict())


class TestFieldCoerce(unittest.TestCase):

    """ Tests Field type coercion """

    def test_always_coerce_str_unicode(self):
        """ Always coerce bytes to unicode """
        field = Field(data_type=six.text_type)
        ret = field.coerce(b'val')
        self.assertTrue(isinstance(ret, six.text_type))

    def test_coerce_unicode(self):
        """ Coerce to unicode """
        field = Field(data_type=six.text_type, coerce=True)
        ret = field.coerce(5)
        self.assertTrue(isinstance(ret, six.text_type))

    def test_coerce_unicode_fail(self):
        """ Coerce to unicode fails if coerce=False """
        field = Field(data_type=six.text_type)
        with self.assertRaises(TypeError):
            field.coerce(5)

    def test_always_coerce_unicode_str(self):
        """ Always coerce unicode to bytes """
        field = Field(data_type=six.binary_type)
        ret = field.coerce(six.u('val'))
        self.assertTrue(isinstance(ret, six.binary_type))

    def test_coerce_str(self):
        """ Coerce to bytes """
        field = Field(data_type=six.binary_type, coerce=True)
        ret = field.coerce(5)
        self.assertTrue(isinstance(ret, six.binary_type))

    def test_coerce_str_fail(self):
        """ Coerce to bytes fails if coerce=False """
        field = Field(data_type=six.binary_type)
        with self.assertRaises(TypeError):
            field.coerce(5)

    def test_int_no_data_loss(self):
        """ Int fields refuse to drop floating point data """
        field = Field(data_type=int, coerce=True)
        with self.assertRaises(ValueError):
            field.coerce(4.5)
        with self.assertRaises(ValueError):
            field.coerce(Decimal('4.5'))

    def test_int_coerce(self):
        """ Int fields can coerce floats """
        field = Field(data_type=int, coerce=True)
        ret = field.coerce(4.0)
        self.assertEquals(ret, 4)
        self.assertTrue(isinstance(ret, int))

    def test_int_coerce_fail(self):
        """ Coerce to int fails if coerce=False """
        field = Field(data_type=int)
        with self.assertRaises(TypeError):
            field.coerce(4.0)

    def test_int_coerce_long(self):
        """ Int fields can transparently handle longs """
        field = Field(data_type=int)
        val = 100
        ret = field.coerce(val)
        self.assertEqual(ret, val)

    def test_coerce_float(self):
        """ Coerce to float """
        field = Field(data_type=float, coerce=True)
        ret = field.coerce('4.3')
        self.assertTrue(isinstance(ret, float))

    def test_always_coerce_int_float(self):
        """ Always coerce ints to float """
        field = Field(data_type=float)
        ret = field.coerce(5)
        self.assertTrue(isinstance(ret, float))

    def test_coerce_float_fail(self):
        """ Coerce to float fails if coerce=False """
        field = Field(data_type=float)
        with self.assertRaises(TypeError):
            field.coerce('4.3')

    def test_coerce_decimal(self):
        """ Coerce to Decimal """
        field = Field(data_type=Decimal, coerce=True)
        ret = field.coerce(5.5)
        self.assertTrue(isinstance(ret, Decimal))

    def test_coerce_decimal_fail(self):
        """ Coerce to Decimal fails if coerce=False """
        field = Field(data_type=Decimal)
        with self.assertRaises(TypeError):
            field.coerce(5.5)

    def test_coerce_set(self):
        """ Coerce to set """
        field = Field(data_type=set, coerce=True)
        ret = field.coerce([1, 2])
        self.assertTrue(isinstance(ret, set))

    def test_coerce_set_fail(self):
        """ Coerce to set fails if coerce=False """
        field = Field(data_type=set)
        with self.assertRaises(TypeError):
            field.coerce([1, 2])

    def test_coerce_dict(self):
        """ Coerce to dict """
        field = Field(data_type=dict, coerce=True)
        ret = field.coerce([(1, 2)])
        self.assertTrue(isinstance(ret, dict))

    def test_coerce_dict_fail(self):
        """ Coerce to dict fails if coerce=False """
        field = Field(data_type=dict)
        with self.assertRaises(TypeError):
            field.coerce([(1, 2)])

    def test_coerce_list(self):
        """ Coerce to list """
        field = Field(data_type=list, coerce=True)
        ret = field.coerce(set([1, 2]))
        self.assertTrue(isinstance(ret, list))

    def test_coerce_list_fail(self):
        """ Coerce to list fails if coerce=False """
        field = Field(data_type=list)
        with self.assertRaises(TypeError):
            field.coerce(set([1, 2]))

    def test_coerce_bool(self):
        """ Coerce to bool """
        field = Field(data_type=bool, coerce=True)
        ret = field.coerce(2)
        self.assertTrue(isinstance(ret, bool))

    def test_coerce_bool_fail(self):
        """ Coerce to bool fails if coerce=False """
        field = Field(data_type=bool)
        with self.assertRaises(TypeError):
            field.coerce(2)

    def test_coerce_datetime_fail(self):
        """ Coercing to datetime fails """
        field = Field(data_type=datetime, coerce=True)
        with self.assertRaises(TypeError):
            field.coerce(12345)

    def test_coerce_date_fail(self):
        """ Coercing to date fails """
        field = Field(data_type=date, coerce=True)
        with self.assertRaises(TypeError):
            field.coerce(12345)

    def test_coerce_basic_set(self):
        """ Coerce to an untyped set """
        field = Field(data_type=set, coerce=True)
        ret = field.coerce(['a', 'b'])
        self.assertEqual(ret, set(['a', 'b']))

    def test_coerce_basic_set_fail(self):
        """ Coercing to untyped set fails """
        field = Field(data_type=set)
        with self.assertRaises(TypeError):
            field.coerce(['a', 'b'])

    def test_coerce_number_set(self):
        """ Coerce to number set """
        field = Field(data_type=set_(int), coerce=True)
        ret = field.coerce([2, '4'])
        self.assertEqual(ret, set([2, 4]))

    def test_coerce_number_set_fail(self):
        """ Coerce to number set fails """
        field = Field(data_type=set_(int))
        with self.assertRaises(TypeError):
            field.coerce([2, '4'])

    def test_coerce_binary_set(self):
        """ Coerce to binary set """
        field = Field(data_type=set_(six.binary_type), coerce=True)
        ret = field.coerce([six.u('hello')])
        self.assertEqual(ret, set([b'hello']))

    def test_set_defn_with_frozenset(self):
        """ Can use frozenset as data type for set fields """
        field = Field(data_type=frozenset([date]))
        self.assertEqual(field.data_type.item_type, date)


class TestFields(DynamoSystemTest):

    """ Tests for fields """
    models = [Widget]

    def test_field_default(self):
        """ If fields are not set, they default to None """
        w = Widget()
        self.assertIsNone(w.string2)
        self.assertIsNone(w.binary)
        self.assertIsNone(w.num)
        self.assertEquals(w.str_set, set())
        self.assertEquals(w.num_set, set())
        self.assertEquals(w.bin_set, set())
        self.assertIsNone(w.data_dict)
        self.assertIsNone(w.data_list)
        self.assertIsNone(w.bigdata)

    def test_valid_check(self):
        """ Widget saves if validation checks pass """
        w = Widget(natural_num=5)
        self.engine.save(w)

    def test_invalid_check(self):
        """ Widget raises error on save if validation checks fail """
        w = Widget(natural_num=-5)
        with self.assertRaises(ValueError):
            self.engine.save(w)

    def test_multiple_valid_check(self):
        """ Widget saves if all validation checks pass """
        w = Widget(check_num=5)
        self.engine.save(w)

    def test_multiple_invalid_check(self):
        """ Widget raises error on save if any validation check fails """
        w = Widget(check_num=2)
        with self.assertRaises(ValueError):
            self.engine.save(w)

    def test_not_nullable(self):
        """ Nullable=False prevents null values """
        w = Widget(not_null=None)
        with self.assertRaises(ValueError):
            self.engine.save(w)

    def test_not_null_other_checks(self):
        """ Nullable=False is appended to other checks """
        w = Widget(not_null_natural=None)
        with self.assertRaises(ValueError):
            self.engine.save(w)

    def test_other_checks(self):
        """ Nullable=False doesn't interfere with other checks """
        w = Widget(not_null_natural=1)
        with self.assertRaises(ValueError):
            self.engine.save(w)

    def test_save_defaults(self):
        """ Default field values are saved to dynamo """
        w = Widget(string2='abc')
        self.engine.sync(w)
        tablename = Widget.meta_.ddb_tablename(self.engine.namespace)
        result = six.next(self.dynamo.scan(tablename))
        self.assertEquals(result, {
            'string': w.string,
            'string2': w.string2,
            'natural_num': 1,
            'not_null': 0,
            'not_null_natural': 0,
        })

    def test_set_updates(self):
        """ Sets track changes and update during sync() """
        w = Widget(string='a')
        w.str_set = set()
        self.engine.save(w)
        w.str_set.add('hi')
        w.sync()
        stored_widget = self.engine.scan(Widget).all()[0]
        self.assertEquals(stored_widget.str_set, set(['hi']))

    def test_set_updates_fetch(self):
        """ Items retrieved from db have sets that track changes """
        w = Widget(string='a', str_set=set(['hi']))
        self.engine.save(w)
        w = self.engine.scan(Widget).all()[0]
        w.str_set.add('foo')
        w.sync()
        stored_widget = self.engine.scan(Widget).all()[0]
        self.assertEquals(w.str_set, stored_widget.str_set)

    def test_set_updates_replace(self):
        """ Replaced sets also track changes for updates """
        w = Widget(string='a')
        w.str_set = set(['hi'])
        self.engine.sync(w)
        w.str_set.add('foo')
        w.sync()
        stored_widget = self.engine.scan(Widget).all()[0]
        self.assertEquals(w.str_set, stored_widget.str_set)

    def test_store_extra_number(self):
        """ Extra number fields are stored as numbers """
        w = Widget(string='a', foobar=5)
        self.engine.sync(w)

        tablename = Widget.meta_.ddb_tablename(self.engine.namespace)
        result = six.next(self.dynamo.scan(tablename))
        self.assertEquals(result['foobar'], 5)
        stored_widget = self.engine.scan(Widget).all()[0]
        self.assertEquals(stored_widget.foobar, 5)

    def test_store_extra_string(self):
        """ Extra string fields are stored as json strings """
        w = Widget(string='a', foobar='hi')
        self.engine.sync(w)

        tablename = Widget.meta_.ddb_tablename(self.engine.namespace)
        result = six.next(self.dynamo.scan(tablename))
        self.assertEquals(result['foobar'], json.dumps('hi'))
        stored_widget = self.engine.scan(Widget).all()[0]
        self.assertEquals(stored_widget.foobar, 'hi')

    def test_store_extra_set(self):
        """ Extra set fields are stored as sets """
        foobar = set(['hi'])
        w = Widget(string='a', foobar=foobar)
        self.engine.sync(w)

        tablename = Widget.meta_.ddb_tablename(self.engine.namespace)
        result = six.next(self.dynamo.scan(tablename))
        self.assertEquals(result['foobar'], foobar)
        stored_widget = self.engine.scan(Widget).all()[0]
        self.assertEquals(stored_widget.foobar, foobar)

    def test_store_extra_dict(self):
        """ Extra dict fields are stored as json strings """
        foobar = {'foo': 'bar'}
        w = Widget(string='a', foobar=foobar)
        self.engine.save(w)

        tablename = Widget.meta_.ddb_tablename(self.engine.namespace)
        result = six.next(self.dynamo.scan(tablename))
        self.assertEquals(result['foobar'], json.dumps(foobar))
        stored_widget = self.engine.scan(Widget).all()[0]
        self.assertEquals(stored_widget.foobar, foobar)

    def test_convert_overflow_int(self):
        """ Should convert overflow ints from Decimal when loading """
        w = Widget(string='a')
        w.foobar = 1
        self.engine.save(w)

        fetched = self.engine.scan(Widget).first()
        self.assertEqual(fetched.foobar, 1)
        self.assertTrue(isinstance(fetched.foobar, int))

    def test_convert_overflow_float(self):
        """ Should convert overflow floats from Decimal when loading """
        w = Widget(string='a')
        w.foobar = 1.3
        self.engine.save(w)

        fetched = self.engine.scan(Widget).first()
        self.assertEqual(fetched.foobar, 1.3)
        self.assertTrue(isinstance(fetched.foobar, float))

    def test_dict_updates(self):
        """ Dicts track changes and update during sync() """
        w = Widget(string='a')
        w.data_dict = {}
        self.engine.save(w)
        w.data_dict['a'] = 'b'
        w.sync()
        stored_widget = self.engine.scan(Widget).first()
        self.assertEquals(stored_widget.data_dict, {'a': 'b'})

    def test_list_updates(self):
        """ Lists track changes and update during sync() """
        w = Widget(string='a')
        w.data_list = []
        self.engine.save(w)
        w.data_list.append('a')
        w.sync()
        stored_widget = self.engine.scan(Widget).first()
        self.assertEquals(stored_widget.data_list, ['a'])

    def test_overflow_set_updates(self):
        """ Overflow sets track changes and update during sync() """
        w = Widget(string='a')
        w.myset = set(['a'])
        self.engine.save(w)
        w.myset.add('b')
        w.sync()
        stored_widget = self.engine.scan(Widget).first()
        self.assertEquals(stored_widget.myset, set(['a', 'b']))

    def test_overflow_dict_updates(self):
        """ Overflow dicts track changes and update during sync() """
        w = Widget(string='a')
        w.mydict = {'a': 'b'}
        self.engine.save(w)
        w.mydict['c'] = 'd'
        w.sync()
        stored_widget = self.engine.scan(Widget).first()
        self.assertEquals(stored_widget.mydict, {'a': 'b', 'c': 'd'})

    def test_overflow_list_updates(self):
        """ Overflow lists track changes and update during sync() """
        w = Widget(string='a')
        w.mylist = ['a']
        self.engine.save(w)
        w.mylist.append('b')
        w.sync()
        stored_widget = self.engine.scan(Widget).first()
        self.assertEquals(stored_widget.mylist, ['a', 'b'])

    def test_empty_set_save(self):
        """ Models can initialize an empty set and saving will work fine """
        w = Widget(str_set=set())
        self.engine.save(w)
        stored_widget = self.engine.scan(Widget).first()
        self.assertEqual(stored_widget.str_set, set())

    def test_empty_set_sync(self):
        """ Models can initialize an empty set and syncing will work fine """
        w = Widget(str_set=set())
        self.engine.sync(w)
        stored_widget = self.engine.scan(Widget).first()
        self.assertEqual(stored_widget.str_set, set())

    def test_custom_field(self):
        """ Can save and load a custom field type """
        w = Widget(bigdata={'a': 1})
        self.engine.save(w)
        stored_widget = self.engine.scan(Widget).first()
        self.assertEqual(stored_widget.bigdata, {'a': 1})


class PrimitiveWidget(Model):

    """ Model for testing python data types """
    __metadata__ = {
        'global_indexes': [
            GlobalIndex('gindex', 'string2', 'num'),
            GlobalIndex('gindex2', 'num2', 'binary'),
        ],
    }
    string = Field(data_type=six.binary_type, hash_key=True)
    string2 = Field(data_type=six.text_type)
    num = Field(data_type=int, coerce=True)
    num2 = Field(data_type=float)
    binary = Field(data_type=BINARY, coerce=True)
    myset = Field(data_type=set)
    data = Field(data_type=dict)
    friends = Field(data_type=list)
    created = Field(data_type=datetime)
    birthday = Field(data_type=date)
    wobbles = Field(data_type=bool)
    price = Field(data_type=Decimal)

    def __init__(self, **kwargs):
        kwargs.setdefault('string', 'abc')
        super(PrimitiveWidget, self).__init__(**kwargs)


class TestPrimitiveDataTypes(DynamoSystemTest):

    """ Tests for default values """
    models = [PrimitiveWidget]

    def test_field_default(self):
        """ If fields are not set, they default to None """
        w = PrimitiveWidget()
        self.assertIsNone(w.string2)
        self.assertIsNone(w.binary)
        self.assertIsNone(w.num)
        self.assertIsNone(w.num2)
        self.assertEquals(w.myset, set())
        self.assertIsNone(w.data)
        self.assertIsNone(w.wobbles)
        self.assertIsNone(w.friends)
        self.assertIsNone(w.created)
        self.assertIsNone(w.birthday)
        self.assertIsNone(w.price)

    def test_dict_updates(self):
        """ Dicts track changes and update during sync() """
        w = PrimitiveWidget(string='a')
        w.data = {}
        self.engine.save(w)
        w.data['foo'] = 'bar'
        w.sync()
        stored_widget = self.engine.scan(PrimitiveWidget).one()
        self.assertEquals(w.data, stored_widget.data)

    def test_datetime(self):
        """ Can store datetime & it gets returned as datetime """
        now = datetime.utcnow().replace(tzinfo=UTC)
        w = PrimitiveWidget(string='a', created=now)
        self.engine.sync(w)
        stored_widget = self.engine.scan(PrimitiveWidget).one()
        self.assertEquals(now, w.created)
        self.assertEquals(now, stored_widget.created)

    def test_date(self):
        """ Can store date & it gets returned as date """
        w = PrimitiveWidget(string='a', birthday=date.today())
        self.engine.sync(w)
        stored_widget = self.engine.scan(PrimitiveWidget).one()
        self.assertEquals(w.birthday, stored_widget.birthday)

    def test_decimal(self):
        """ Can store decimal & it gets returned as decimal """
        w = PrimitiveWidget(string='a', price=Decimal('3.50'))
        self.engine.sync(w)
        stored_widget = self.engine.scan(PrimitiveWidget).one()
        self.assertEquals(w.price, stored_widget.price)
        self.assertTrue(isinstance(stored_widget.price, Decimal))

    def test_list_updates(self):
        """ Lists track changes and update during sync() """
        w = PrimitiveWidget(string='a')
        w.friends = []
        self.engine.save(w)
        w.friends.append('Fred')  # pylint: disable=E1101
        w.sync()
        stored_widget = self.engine.scan(PrimitiveWidget).one()
        self.assertEquals(w.friends, stored_widget.friends)
