""" Tests for fields """
import zlib
from datetime import datetime, date

import json
from decimal import Decimal
from flywheel.fields.types import DictType, S3Type, Key, register_type
from moto import mock_s3

import boto
from . import BaseSystemTest
from flywheel import (Field, Composite, Model, NUMBER, BINARY, STRING_SET,
                      NUMBER_SET, BINARY_SET, Binary, GlobalIndex)


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
        return Binary(zlib.compress(json.dumps(value)))

    def ddb_load(self, value):
        return json.loads(zlib.decompress(value.value))


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
    natural_num = Field(data_type=int, check=lambda x: x > 0, default=1)
    s3data = Composite('string', 'string2', data_type=S3Type('mybucket'),
                       merge=lambda *a: '/'.join([x or '' for x in a]))

    def __init__(self, **kwargs):
        self.string = 'abc'
        for key, val in kwargs.iteritems():
            setattr(self, key, val)


class TestCreateFields(unittest.TestCase):

    """ Tests related to the creation of Fields """

    def test_hash_and_range(self):
        """ A field cannot be both a hash_key and range_key """
        with self.assertRaises(ValueError):
            Field(hash_key=True, range_key=True)

    def test_unknown_data_type(self):
        """ Unknown data types are disallowed by Field """
        with self.assertRaises(TypeError):
            Field(data_type=basestring)

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
        """ Always coerce str to unicode """
        field = Field(data_type=unicode)
        ret = field.coerce(b'val')
        self.assertTrue(isinstance(ret, unicode))

    def test_coerce_unicode(self):
        """ Coerce to unicode """
        field = Field(data_type=unicode, coerce=True)
        ret = field.coerce(5)
        self.assertTrue(isinstance(ret, unicode))

    def test_coerce_unicode_fail(self):
        """ Coerce to unicode fails if coerce=False """
        field = Field(data_type=unicode)
        with self.assertRaises(TypeError):
            field.coerce(5)

    def test_always_coerce_unicode_str(self):
        """ Always coerce unicode to str """
        field = Field(data_type=str)
        ret = field.coerce(u'val')
        self.assertTrue(isinstance(ret, str))

    def test_coerce_str(self):
        """ Coerce to str """
        field = Field(data_type=str, coerce=True)
        ret = field.coerce(5)
        self.assertTrue(isinstance(ret, str))

    def test_coerce_str_fail(self):
        """ Coerce to str fails if coerce=False """
        field = Field(data_type=str)
        with self.assertRaises(TypeError):
            field.coerce(5)

    def test_int_no_data_loss(self):
        """ Int fields refuse to drop floating point data """
        field = Field(data_type=int, coerce=True)
        with self.assertRaises(TypeError):
            field.coerce(4.5)
        with self.assertRaises(TypeError):
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
        val = 100L
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

    @mock_s3
    def test_coerce_s3key(self):
        """ Coerce string to S3 key """
        field = Field(data_type=S3Type('mybucket'))
        ret = field.coerce('my/path')
        self.assertTrue(isinstance(ret, Key))
        self.assertEqual(ret.key, 'my/path')

    @mock_s3
    def test_coerce_boto_s3key(self):
        """ Coerce boto key to S3 key """
        field = Field(data_type=S3Type('mybucket'))
        boto_key = boto.s3.key.Key()
        boto_key.key = 'my/path'
        ret = field.coerce(boto_key)
        self.assertTrue(isinstance(ret, Key))
        self.assertEqual(ret.key, 'my/path')


class TestFields(BaseSystemTest):

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

    def test_save_defaults(self):
        """ Default field values are saved to dynamo """
        w = Widget(string2='abc')
        self.engine.sync(w)
        table = w.meta_.ddb_table(self.dynamo)
        result = dict(list(table.scan())[0])
        self.assertEquals(result, {
            'string': w.string,
            'string2': w.string2,
            'natural_num': 1,
            's3data': w.string + '/' + w.string2,
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

        table = Widget.meta_.ddb_table(self.dynamo)
        result = list(table.scan())[0]
        self.assertEquals(result['foobar'], 5)
        stored_widget = self.engine.scan(Widget).all()[0]
        self.assertEquals(stored_widget.foobar, 5)

    def test_store_extra_string(self):
        """ Extra string fields are stored as json strings """
        w = Widget(string='a', foobar='hi')
        self.engine.sync(w)

        table = Widget.meta_.ddb_table(self.dynamo)
        result = list(table.scan())[0]
        self.assertEquals(result['foobar'], json.dumps('hi'))
        stored_widget = self.engine.scan(Widget).all()[0]
        self.assertEquals(stored_widget.foobar, 'hi')

    def test_store_extra_set(self):
        """ Extra set fields are stored as sets """
        foobar = set(['hi'])
        w = Widget(string='a', foobar=foobar)
        self.engine.sync(w)

        table = Widget.meta_.ddb_table(self.dynamo)
        result = list(table.scan())[0]
        self.assertEquals(result['foobar'], foobar)
        stored_widget = self.engine.scan(Widget).all()[0]
        self.assertEquals(stored_widget.foobar, foobar)

    def test_store_extra_dict(self):
        """ Extra dict fields are stored as json strings """
        foobar = {'foo': 'bar'}
        w = Widget(string='a', foobar=foobar)
        self.engine.save(w)

        table = Widget.meta_.ddb_table(self.dynamo)
        result = list(table.scan())[0]
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

    @mock_s3
    def test_s3_data(self):
        """ Can save and retrieve S3 data """
        conn = boto.connect_s3()
        conn.create_bucket('mybucket')
        w = Widget(string='a', string2='b')
        w.s3data.set_contents_from_string('data')
        self.engine.save(w)
        stored_widget = self.engine.scan(Widget).first()
        self.assertEqual(stored_widget.s3data.key, 'a/b')
        data = stored_widget.s3data.get_contents_as_string()
        self.assertEqual(data, 'data')


class PrimitiveWidget(Model):

    """ Model for testing python data types """
    __metadata__ = {
        'global_indexes': [
            GlobalIndex('gindex', 'string2', 'num'),
            GlobalIndex('gindex2', 'num2', 'binary'),
        ],
    }
    string = Field(data_type=str, hash_key=True)
    string2 = Field(data_type=unicode)
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
        self.string = 'abc'
        for key, val in kwargs.iteritems():
            setattr(self, key, val)


class TestPrimitiveDataTypes(BaseSystemTest):

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
        stored_widget = self.engine.scan(PrimitiveWidget).all()[0]
        self.assertEquals(w.data, stored_widget.data)

    def test_store_bool(self):
        """ Dicts track changes and update during sync() """
        w = PrimitiveWidget(string='a', wobbles=True)
        self.engine.sync(w)
        stored_widget = self.engine.scan(PrimitiveWidget).all()[0]
        self.assertTrue(stored_widget.wobbles is True)

    def test_datetime(self):
        """ Can store datetime & it gets returned as datetime """
        w = PrimitiveWidget(string='a', created=datetime.utcnow())
        self.engine.sync(w)
        stored_widget = self.engine.scan(PrimitiveWidget).all()[0]
        self.assertEquals(w.created, stored_widget.created)

    def test_date(self):
        """ Can store date & it gets returned as date """
        w = PrimitiveWidget(string='a', birthday=date.today())
        self.engine.sync(w)
        stored_widget = self.engine.scan(PrimitiveWidget).all()[0]
        self.assertEquals(w.birthday, stored_widget.birthday)

    def test_decimal(self):
        """ Can store decimal & it gets returned as decimal """
        w = PrimitiveWidget(string='a', price=Decimal('3.50'))
        self.engine.sync(w)
        stored_widget = self.engine.scan(PrimitiveWidget).all()[0]
        self.assertEquals(w.price, stored_widget.price)
        self.assertTrue(isinstance(stored_widget.price, Decimal))

    def test_list_updates(self):
        """ Lists track changes and update during sync() """
        w = PrimitiveWidget(string='a')
        w.friends = []
        self.engine.save(w)
        w.friends.append('Fred')  # pylint: disable=E1101
        w.sync()
        stored_widget = self.engine.scan(PrimitiveWidget).all()[0]
        self.assertEquals(w.friends, stored_widget.friends)
