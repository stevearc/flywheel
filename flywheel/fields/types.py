""" Field type definitions """
import calendar
import datetime
import functools
import json
import six
from decimal import Decimal
from dynamo3 import (Binary, NUMBER, STRING, BINARY, NUMBER_SET, STRING_SET,
                     BINARY_SET, BOOL, MAP, LIST)
from dynamo3.types import float_to_decimal

from flywheel.compat import UnicodeMixin

ALL_TYPES = {}


def set_(data_type):
    """ Create an alias for a SetType that contains this data type """
    return frozenset([data_type])


def register_type(type_class, allow_in_set=True):
    """ Register a type class for use with Fields """
    ALL_TYPES[type_class.data_type] = type_class
    if allow_in_set:
        set_key = set_(type_class.data_type)
        ALL_TYPES[set_key] = SetType.bind(type_class.data_type)
    for alias in type_class.aliases:
        ALL_TYPES[alias] = type_class
        ALL_TYPES[set_(alias)] = SetType.bind(alias)


class TypeDefinition(UnicodeMixin):

    """
    Base class for all Field types

    Attributes
    ----------
    data_type : object
        The value you wish to pass in to Field as the data_type.
    aliases : list
        Other values that will reference this type if passed to Field
    ddb_data_type : {STRING, BINARY, NUMBER, STRING_SET, BINARY_SET, NUMBER_SET, BOOL, LIST, MAP}
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
        elif self.ddb_data_type in (MAP, BOOL):
            self.allowed_filters = set(['eq', 'ne'])
        elif self.ddb_data_type == LIST:
            self.allowed_filters = set(['eq', 'ne', 'contains', 'ncontains'])
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

    def ddb_dump_inner(self, value):
        """
        If this is a set type, dump a value to the type contained in the set

        """
        return value

    def ddb_dump(self, value):
        """ Dump a value to a form that can be stored in DynamoDB """
        return value

    def ddb_load(self, value):
        """ Turn a value into this type from a DynamoDB value """
        return value

    def _attempt_coerce_json(self, value, obj_type):
        """
        If a value was previously stored as json, attempt to load it as an
        obj_type object
        """
        if isinstance(value, six.text_type):
            orig_value = value
            try:
                value = json.loads(orig_value)
                if not isinstance(value, obj_type):
                    value = orig_value
            except Exception as e:
                raise TypeError(e)
        return value

    def __repr__(self):
        return 'TypeDefinition(%s)' % self

    def __unicode__(self):
        return six.text_type(self.data_type)


class SetType(TypeDefinition):

    """ Set types """
    data_type = set
    mutable = True

    def __init__(self, item_type=None, type_class=None):
        self.item_type = item_type
        set_map = {
            STRING: STRING_SET,
            NUMBER: NUMBER_SET,
            BINARY: BINARY_SET,
        }
        if item_type is not None and type_class is None:
            self.item_field = ALL_TYPES[item_type]()
            self.ddb_data_type = set_map[self.item_field.ddb_data_type]
        elif type_class is not None:
            self.item_field = type_class()
            self.ddb_data_type = set_map[self.item_field.ddb_data_type]
        else:
            self.item_field = None
            self.ddb_data_type = STRING_SET
        super(SetType, self).__init__()

    def coerce(self, value, force):
        if not isinstance(value, set):
            if force:
                value = set(value)
            else:
                raise TypeError()
        if self.item_field is not None:
            converted_values = set()
            for item in value:
                converted_values.add(self.item_field.coerce(item, force))
            return converted_values
        return value

    def ddb_dump_inner(self, value):
        """ We need to expose this for 'contains' and 'ncontains' """
        if self.item_field is None:
            return value
        return self.item_field.ddb_dump(value)

    def ddb_dump(self, value):
        if self.item_field is None:
            return value
        return set([self.ddb_dump_inner(v) for v in value])

    def ddb_load(self, value):
        if self.item_field is None:
            return value
        return set([self.item_field.ddb_load(v) for v in value])

    def __unicode__(self):
        if self.item_type is None:
            return super(SetType, self).__unicode__()
        else:
            return six.text_type(set([self.item_type]))

    @classmethod
    def bind(cls, item_type):
        """ Create a set factory that will contain a specific data type """
        return functools.partial(cls, item_type)

register_type(SetType, allow_in_set=False)
ALL_TYPES[STRING_SET] = SetType.bind(STRING)
ALL_TYPES[BINARY_SET] = SetType.bind(BINARY)
ALL_TYPES[NUMBER_SET] = SetType.bind(NUMBER)


class NumberType(TypeDefinition):

    """ Any kind of numerical value """
    data_type = NUMBER
    ddb_data_type = NUMBER

    def coerce(self, value, force):
        if not (isinstance(value, float) or isinstance(value, Decimal) or
                isinstance(value, six.integer_types)):
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
            if (isinstance(value, six.integer_types) or
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
    aliases = list(six.integer_types)
    ddb_data_type = NUMBER

    def coerce(self, value, force):
        if not isinstance(value, six.integer_types):
            if force:
                new_val = int(value)
                if isinstance(value, float) or isinstance(value, Decimal):
                    if new_val != value:
                        raise ValueError("Refusing to convert "
                                         "%r to int! Results in data loss!"
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

    """ Boolean type """

    data_type = bool
    ddb_data_type = BOOL

    def __init__(self):
        super(BoolType, self).__init__()

    def coerce(self, value, force):
        value = self._attempt_coerce_json(value, bool)
        if not isinstance(value, bool):
            if force:
                return bool(value)
            else:
                raise TypeError()
        return value

register_type(BoolType)


class StringType(TypeDefinition):

    """ String values, stored as unicode """
    data_type = six.text_type
    aliases = [STRING]
    ddb_data_type = STRING

    def coerce(self, value, force):
        if not isinstance(value, six.text_type):
            # Silently convert str to unicode using utf-8
            if isinstance(value, six.binary_type):
                return value.decode('utf-8')
            if force:
                return six.text_type(value)
            else:
                raise TypeError()
        return value

register_type(StringType)


class BinaryType(TypeDefinition):

    """ Binary strings, stored as a str/bytes """
    data_type = six.binary_type
    aliases = [BINARY, Binary]
    ddb_data_type = BINARY

    def coerce(self, value, force):
        if not isinstance(value, six.binary_type):
            # Silently convert unicode to str using utf-8
            if isinstance(value, six.text_type):
                return value.encode('utf-8')
            if force:
                return six.binary_type(value)
            else:
                raise TypeError()
        return value

    def ddb_dump(self, value):
        return Binary(value)

    def ddb_load(self, value):
        return value.value

register_type(BinaryType)


class DictType(TypeDefinition):

    """ Dict type, stored as a map """
    data_type = dict
    ddb_data_type = MAP
    mutable = True

    def __init__(self):
        super(DictType, self).__init__()

    def coerce(self, value, force):
        value = self._attempt_coerce_json(value, dict)
        if not isinstance(value, dict):
            if force:
                return dict(value)
            else:
                raise TypeError()
        return value

register_type(DictType)


class ListType(TypeDefinition):

    """ List type """
    data_type = list
    ddb_data_type = LIST
    mutable = True

    def __init__(self):
        super(ListType, self).__init__()

    def coerce(self, value, force):
        value = self._attempt_coerce_json(value, list)
        if not isinstance(value, list):
            if force:
                return list(value)
            else:
                raise TypeError()
        return value

register_type(ListType)


ZERO = datetime.timedelta(0)

# https://docs.python.org/2/library/datetime.html#datetime.tzinfo.fromutc


class UTCTimezone(datetime.tzinfo):

    """ UTC """

    def utcoffset(self, dt):
        return ZERO

    def tzname(self, dt):
        return 'UTC'

    def dst(self, dt):
        return ZERO

UTC = UTCTimezone()


class DateTimeType(TypeDefinition):

    """
    Datetimes, stored as a unix timestamp

    Parameters
    ----------
    naive : bool, optional
        If True, will load values from Dynamo with no timezone. If False, will
        add a UTC timezone. (Default False).

    Notes
    -----
    If you want to use naive datetimes, you will need to reference the type
    class directly instead of going through an alias. For example:

    .. code-block:: python

        from flywheel.fields.types import DateTimeType

        field = Field(data_type=DateTimeType(naive=True))

    """
    data_type = datetime.datetime
    ddb_data_type = NUMBER

    def __init__(self, naive=False):
        super(DateTimeType, self).__init__()
        self.naive = naive

    def ddb_dump(self, value):
        seconds = calendar.timegm(value.utctimetuple())
        milliseconds = value.strftime('%f')
        return Decimal("%d.%s" % (seconds, milliseconds))

    def ddb_load(self, value):
        microseconds = int(1000000 * (value - int(value)))
        dt = datetime.datetime.utcfromtimestamp(value) \
            .replace(microsecond=microseconds)
        if self.naive:
            return dt
        else:
            return dt.replace(tzinfo=UTC)

register_type(DateTimeType)


class DateType(TypeDefinition):

    """ Dates, stored as timestamps """
    data_type = datetime.date
    ddb_data_type = NUMBER

    def ddb_dump(self, value):
        return calendar.timegm(value.timetuple())

    def ddb_load(self, value):
        return datetime.datetime.utcfromtimestamp(value).date()


register_type(DateType)
