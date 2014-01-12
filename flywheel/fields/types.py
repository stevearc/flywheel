""" Field type definitions """
import datetime

import json
import boto.s3.key
from boto.dynamodb.types import Binary, float_to_decimal
from boto.dynamodb2.types import (NUMBER, STRING, BINARY, NUMBER_SET,
                                  STRING_SET, BINARY_SET)
from decimal import Decimal


ALL_TYPES = {}


def register_type(type_class):
    """ Register a type class for use with Fields """
    ALL_TYPES[type_class.data_type] = type_class
    for alias in type_class.aliases:
        ALL_TYPES[alias] = type_class


class TypeDefinition(object):

    """
    Base class for all Field types

    Attributes
    ----------
    data_type : object
        The value you wish to pass in to Field as the data_type.
    aliases : list
        Other values that will reference this type if passed to Field
    ddb_data_type : {STRING, BINARY, NUMBER, STRING_SET, BINARY_SET, NUMBER_SET}
        The DynamoDB data type that backs this type
    mutable : bool
        If True, flywheel will track updates to this field automatically when
        making calls to sync()
    allowed_filters : set
        The set of filters that can be used on this field type

    """
    data_type = None
    aliases = []
    ddb_data_type = None
    mutable = False

    def __init__(self):
        if self.ddb_data_type == NUMBER:
            self.allowed_filters = set(['eq', 'ne', 'lte', 'lt', 'gte', 'gt',
                                        'in', 'between'])
        elif self.ddb_data_type in (STRING, BINARY):
            self.allowed_filters = set(['eq', 'ne', 'lte', 'lt', 'gte', 'gt',
                                        'in', 'between', 'beginswith'])
        elif self.ddb_data_type in (NUMBER_SET, STRING_SET, BINARY_SET):
            self.allowed_filters = set(['contains', 'ncontains', 'in'])
        else:
            raise ValueError("Unknown dynamo data type '%s'" %
                             self.ddb_data_type)

    def coerce(self, value, force):
        """
        Check the type of a value and possible convert it

        Parameters
        ----------
        value : object
            The value to check
        force : bool
            If True, always attempt to convert a bad type to the correct type

        Returns
        -------
        value : object
            A variable of the correct type

        Raises
        ------
        exc : TypeError or ValueError
            If the value is the incorrect type and could not be converted

        """

        if not isinstance(value, self.data_type):
            raise TypeError()
        return value

    def ddb_dump(self, value):
        """ Dump a value to a form that can be stored in DynamoDB """
        return value

    def ddb_load(self, value):
        """ Turn a value into this type from a DynamoDB value """
        return value

    def __repr__(self):
        return u'TypeDefinition(%s)' % self.data_type

    def __unicode__(self):
        return unicode(self.data_type)

    def __str__(self):
        return unicode(self).encode('utf-8')


class NumberType(TypeDefinition):

    """ Any kind of numerical value """
    data_type = NUMBER
    ddb_data_type = NUMBER

    def coerce(self, value, force):
        if not (isinstance(value, float) or isinstance(value, int) or
                isinstance(value, long) or isinstance(value, Decimal)):
            if force:
                try:
                    return int(value)
                except ValueError:
                    return float(value)
            else:
                raise TypeError()
        return value

    def ddb_load(self, value):
        if isinstance(value, Decimal):
            if value % 1 == 0:
                return int(value)
            else:
                return float(value)
        return value

register_type(NumberType)


class FloatType(TypeDefinition):

    """ Float values """
    data_type = float
    ddb_data_type = NUMBER

    def coerce(self, value, force):
        if not isinstance(value, float):
            # Auto-convert ints, longs, and Decimals
            if (isinstance(value, int) or isinstance(value, long) or
                    isinstance(value, Decimal)):
                return float(value)
            elif force:
                return float(value)
            else:
                raise TypeError()
        return value

    def ddb_load(self, value):
        return float(value)

register_type(FloatType)


class IntType(TypeDefinition):

    """ Integer values (includes longs) """
    data_type = int
    ddb_data_type = NUMBER

    def coerce(self, value, force):
        if not (isinstance(value, int) or isinstance(value, long)):
            if force:
                new_val = int(value)
                if isinstance(value, float) or isinstance(value, Decimal):
                    if new_val != value:
                        raise ValueError("Refusing to convert "
                                         "%s to int! Results in data loss!"
                                         % repr(value))
                return new_val
            else:
                raise TypeError()
        return value

    def ddb_load(self, value):
        return int(value)

register_type(IntType)


class DecimalType(TypeDefinition):

    """
    Numerical values that use Decimal in the application layer.

    This should be used if you want to work with floats but need the additional
    precision of the Decimal type.

    """

    data_type = Decimal
    ddb_data_type = NUMBER

    def coerce(self, value, force):
        if not isinstance(value, Decimal):
            if force:
                # Python 2.6 can't convert directly from float to Decimal
                if (isinstance(value, float) and
                        not hasattr(Decimal, 'from_float')):
                    return float_to_decimal(value)
                return Decimal(value)
            else:
                raise TypeError()
        return value

register_type(DecimalType)


class BoolType(TypeDefinition):

    """ Booleans, backed by a Dynamo Number """

    data_type = bool
    ddb_data_type = NUMBER

    def __init__(self):
        super(BoolType, self).__init__()
        self.allowed_filters = set(['eq', 'ne'])

    def coerce(self, value, force):
        if not isinstance(value, bool):
            if force:
                return bool(value)
            else:
                raise TypeError()
        return value

    def ddb_dump(self, value):
        return int(value)

    def ddb_load(self, value):
        return bool(value)

register_type(BoolType)


class StringType(TypeDefinition):

    """ String values, stored as unicode """
    data_type = unicode
    aliases = [STRING]
    ddb_data_type = STRING

    def coerce(self, value, force):
        if not isinstance(value, unicode):
            # Silently convert str to unicode using utf-8
            if isinstance(value, str):
                return value.decode('utf-8')
            if force:
                return unicode(value)
            else:
                raise TypeError()
        return value

register_type(StringType)


class BinaryType(TypeDefinition):

    """ Binary strings, stored as a str """
    data_type = str
    aliases = [BINARY, Binary, bytes]
    ddb_data_type = BINARY

    def coerce(self, value, force):
        if not isinstance(value, str):
            # Silently convert unicode to str using utf-8
            if isinstance(value, unicode):
                return value.encode('utf-8')
            if force:
                return str(value)
            else:
                raise TypeError()
        return value

    def ddb_dump(self, value):
        return Binary(value)

    def ddb_load(self, value):
        return value.value

register_type(BinaryType)


class SetType(TypeDefinition):

    """ Set types """
    data_type = set
    aliases = [BINARY_SET, STRING_SET, NUMBER_SET]
    # It's fine to just use a single set type because it will never be used in
    # schema definitions (which is the only place we really use ddb_data_type)
    ddb_data_type = STRING_SET
    mutable = True

    def coerce(self, value, force):
        if not isinstance(value, set):
            if force:
                return set(value)
            else:
                raise TypeError()
        return value

register_type(SetType)


class DictType(TypeDefinition):

    """ Dict types, stored as a json string """
    data_type = dict
    ddb_data_type = STRING
    mutable = True

    def __init__(self):
        super(DictType, self).__init__()
        self.allowed_filters = set()

    def coerce(self, value, force):
        if not isinstance(value, dict):
            if force:
                return dict(value)
            else:
                raise TypeError()
        return value

    def ddb_dump(self, value):
        return json.dumps(value)

    def ddb_load(self, value):
        return json.loads(value)

register_type(DictType)


class ListType(TypeDefinition):

    """ List types, stored as a json string """
    data_type = list
    ddb_data_type = STRING
    mutable = True

    def __init__(self):
        super(ListType, self).__init__()
        self.allowed_filters = set()

    def coerce(self, value, force):
        if not isinstance(value, list):
            if force:
                return list(value)
            else:
                raise TypeError()
        return value

    def ddb_dump(self, value):
        return json.dumps(value)

    def ddb_load(self, value):
        return json.loads(value)

register_type(ListType)


class DateTimeType(TypeDefinition):

    """ Datetimes, stored as a unix timestamp """
    data_type = datetime.datetime
    ddb_data_type = NUMBER

    def ddb_dump(self, value):
        return float(value.strftime('%s.%f'))

    def ddb_load(self, value):
        return datetime.datetime.fromtimestamp(value)

register_type(DateTimeType)


class DateType(TypeDefinition):

    """ Dates, stored as timestamps """
    data_type = datetime.date
    ddb_data_type = NUMBER

    def ddb_dump(self, value):
        return int(value.strftime('%s'))

    def ddb_load(self, value):
        return datetime.date.fromtimestamp(value)

register_type(DateType)


class Key(boto.s3.key.Key):

    """ Subclass of boto S3 key that adds equality operators """

    def __hash__(self):
        return hash(self.key)

    def __eq__(self, other):
        return self.key == getattr(other, 'key', None)

    def __ne__(self, other):
        return not self.__eq__(other)


class S3Type(TypeDefinition):

    """
    Store a link to an S3 key

    Parameters
    ----------
    bucket : str
        The name of the S3 bucket
    scheme : str, optional
        The name of the scheme to use to connect to S3 if the bucket is a
        string. (default 'default'). See :meth:`~.set_scheme` for more
        information.

    """
    ddb_data_type = STRING
    mutable = True
    SCHEMES = {
        'default': {},
    }
    _connections = {}

    def __init__(self, bucket, scheme='default'):
        super(S3Type, self).__init__()
        self._bucket_name = bucket
        self._bucket = None
        self._scheme = scheme

    @property
    def bucket(self):
        """ Getter for S3 bucket """
        if self._bucket is None:
            self._bucket = self._get_bucket(self._bucket_name, self._scheme)
        return self._bucket

    @classmethod
    def _get_bucket(cls, bucket, scheme):
        """ Get a connection to an S3 bucket """
        if scheme not in cls.SCHEMES:
            raise KeyError("Could not find S3 connection scheme '%s'. "
                           "Have you registered it with S3Type.set_scheme?"
                           % scheme)
        if scheme not in cls._connections:
            cls._connections[scheme] = boto.connect_s3(**cls.SCHEMES[scheme])
        return cls._connections[scheme].get_bucket(bucket, validate=False)

    @classmethod
    def set_scheme(cls, name, **kwargs):
        """
        Register a S3 connection scheme

        The connection scheme is a collection of keyword arguments that will be
        passed to :meth:`~boto.connect_s3` when creating a S3 connection.

        Parameters
        ----------
        name : str
            The name of the scheme. If the name is 'default', then that scheme
            will be used when no scheme is explicitly passed to the
            constructor.
        **kwargs : dict
            All keyword arguments

        """
        cls.SCHEMES[name] = kwargs

    def coerce(self, value, force):
        """ S3Type will auto-coerce string types """
        if not isinstance(value, Key):
            if force or isinstance(value, basestring):
                key = Key(self.bucket)
                key.key = value
                return key
            else:
                raise TypeError()
        return value

    def ddb_dump(self, value):
        return value.key

    def ddb_load(self, value):
        key = Key(self.bucket)
        key.key = value
        return key
