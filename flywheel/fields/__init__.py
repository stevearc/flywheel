""" Field declarations for models """
import copy
import six
import inspect
import json
from dynamo3 import (DynamoKey, LocalIndex, NUMBER, STRING, BINARY, NUMBER_SET,
                     STRING_SET, BINARY_SET, is_null)
from decimal import Decimal

from .conditions import Condition
from .indexes import GlobalIndex
from .types import TypeDefinition, ALL_TYPES, set_

NO_ARG = object()


class Field(object):

    """
    Declarative way to specify model fields

    Parameters
    ----------
    hash_key : bool, optional
        This key is a DynamoDB hash key (default False)
    range_key : bool, optional
        This key is a DynamoDB range key (default False)
    index : str, optional
        If present, create a local secondary index on this field with this as
        the name.
    data_type : object, optional
        The field data type. You may use int, unicode, set, etc. or you may
        pass in an instance of :class:`~flywheel.fields.types.TypeDefinition`
        (default unicode)
    coerce : bool, optional
        Attempt to coerce the value if it's the incorrect type (default False)
    check : callable or list, optional
        A function that takes the value and returns True if the value is valid.
        May also be a list of such functions. (default None)
    nullable : bool, optional
        If false, will add a check (above) to ensure the value is not null
        (default True).
    default : object, optional
        The default value for this field that will be set when creating a model
        (default None, except for ``set`` data types which default to set())

    Attributes
    ----------
    name : str
        The name of the attribute on the model
    model : class
        The :class:`.Model` this field is attached to
    composite : bool
        True if this is a composite field

    Notes
    -----
    ::

        Field(index='my-index')

    Is shorthand for::

        Field().all_index('my-index')

    """

    def __init__(self, hash_key=False, range_key=False, index=None,
                 data_type=six.text_type, coerce=False, check=None,
                 nullable=True, default=NO_ARG):
        if hash_key and range_key:
            raise ValueError("hash_key and range_key are mutually exclusive!")
        self.name = None
        self.model = None
        self.composite = False
        self.overflow = False
        if isinstance(data_type, TypeDefinition):
            self.data_type = data_type
        elif (inspect.isclass(data_type) and
              issubclass(data_type, TypeDefinition)):
            self.data_type = data_type()
        else:
            type_factory = ALL_TYPES.get(data_type)
            if type_factory is None:
                raise TypeError("Unrecognized data_type '%s'" % data_type)
            self.data_type = type_factory()
        self._coerce = coerce
        self.check = []
        if check is not None:
            if hasattr(check, '__iter__'):
                self.check = list(check)
            else:
                self.check = [check]
        if not nullable:
            self.check.append(lambda x: not is_null(x))
        self.hash_key = hash_key
        self.range_key = range_key
        self.subfields = []
        self.index = False
        self.index_name = None
        self._ddb_index = None
        self._ddb_index_kwargs = None
        if default is NO_ARG:
            if self.is_set:
                self._default = set()
            else:
                self._default = None
        else:
            self._default = default
        if index:
            self.all_index(index)

    @property
    def default(self):
        """ Get a shallow copy of the default value """
        return copy.copy(self._default)

    def get_ddb_index(self):
        """ Construct a dynamo local index object """
        range_key = DynamoKey(self.name, data_type=self.ddb_data_type)
        return self._ddb_index(self.index_name, range_key,
                               **self._ddb_index_kwargs)

    def _set_ddb_index(self, name, factory, **kwargs):
        """ Set the type of index """
        if self.hash_key or self.range_key:
            raise ValueError("Cannot index the hash or range key!")
        if self.index:
            raise ValueError("Index is already set!")
        self.index_name = name
        self._ddb_index = factory
        self._ddb_index_kwargs = kwargs
        self.index = True

    def validate(self, obj):
        """
        Run the validation checks for this field on a model object.

        Parameters
        ----------
        obj : :class:`~flywheel.models.Model`

        Raises
        ------
        err : :class:`ValueError`
            Raised if any of the checks fail.

        """
        if self.check is None:
            return
        val = self.resolve(obj)
        for check in self.check:
            if not check(val):
                raise ValueError("Validation check on field %s failed for "
                                 "value %r" % (self.name, val))

    def all_index(self, name):
        """
        Index this field and project all attributes

        Parameters
        ----------
        name : str
            The name of the index

        """
        self._set_ddb_index(name, LocalIndex.all)
        return self

    def keys_index(self, name):
        """
        Index this field and project all key attributes

        Parameters
        ----------
        name : str
            The name of the index

        """
        self._set_ddb_index(name, LocalIndex.keys)
        return self

    def include_index(self, name, includes=None):
        """
        Index this field and project selected attributes

        Parameters
        ----------
        name : str
            The name of the index
        includes : list, optional
            List of non-key attributes to project into this index

        """
        includes = includes or []
        self._set_ddb_index(name, LocalIndex.include, includes=includes)
        return self

    def coerce(self, value, force_coerce=None):
        """ Coerce the value to the field's data type """
        if value is None:
            return value
        if force_coerce is None:
            force_coerce = self._coerce
        try:
            return self.data_type.coerce(value, force_coerce)
        except (TypeError, ValueError) as e:
            if e.args:
                raise
            raise TypeError("Field '%s' must be %s! '%s'" %
                            (self.name, self.data_type, repr(value)))

    @property
    def is_mutable(self):
        """ Return True if the data type is mutable """
        return self.data_type.mutable

    @property
    def is_set(self):
        """ Return True if data type is a set """
        return self.ddb_data_type in (STRING_SET, NUMBER_SET, BINARY_SET)

    def ddb_dump(self, value):
        """ Dump a value to its Dynamo format """
        if value is None:
            return None
        return self.data_type.ddb_dump(value)

    def ddb_dump_for_query(self, value):
        """ Dump a value to format for use in a Dynamo query """
        if value is None:
            return None
        if self.overflow:
            return self.ddb_dump_overflow(value)
        value = self.coerce(value, force_coerce=True)
        return self.ddb_dump(value)

    def ddb_load(self, val):
        """ Decode a value retrieved from Dynamo """
        return self.data_type.ddb_load(val)

    @classmethod
    def ddb_dump_overflow(cls, val):
        """ Dump an overflow value to its Dynamo format """
        if val is None:
            return None
        elif isinstance(val, six.integer_types) or isinstance(val, float):
            return val
        elif isinstance(val, set):
            return val
        else:
            return json.dumps(val)

    @classmethod
    def ddb_load_overflow(cls, val):
        """ Decode a value of an overflow field """
        if (isinstance(val, Decimal) or isinstance(val, float) or
                isinstance(val, six.integer_types)):
            if val % 1 == 0:
                return int(val)
            return float(val)
        elif isinstance(val, set):
            return val
        else:
            return json.loads(val)

    @classmethod
    def is_overflow_mutable(cls, val):
        """ Check if an overflow field is mutable """
        if val is None:
            return False
        val_type = type(val)
        if val_type in ALL_TYPES:
            return ALL_TYPES[val_type].mutable
        return True

    @property
    def ddb_data_type(self):
        """ Get the native DynamoDB data type """
        return self.data_type.ddb_data_type

    def can_resolve(self, fields):
        """
        Check if the provided fields are enough to fully resolve this field

        Parameters
        ----------
        fields : list or set

        Returns
        -------
        needed : set
            Set of the subfields needed to resolve this field. If empty, then
            it cannot be resolved.

        """
        needed = set()
        # If this field name is found in the fields, that is all that's needed
        if self.name in fields:
            needed.add(self.name)
        elif self.subfields:
            # Otherwise, we need ALL of the subfields (only non-empty for
            # Composites) to recursively be satisfied
            for field in self.subfields:
                resolve = self.model.meta_.fields[field].can_resolve(fields)
                if not resolve:
                    return set()
                needed.update(resolve)
        return needed

    def resolve(self, obj=None, scope=None):
        """ Resolve a field value from an object or scope dict """
        if obj is not None:
            return getattr(obj, self.name)
        else:
            return scope[self.name]

    def get_cached_value(self, obj):
        """ Get the cached value of a field before any local modifications """
        if self.name in obj.__cache__:
            return obj.__cache__[self.name]
        else:
            return self.resolve(obj)

    def _make_condition(self, filter, other):
        """
        Construct a query condition for a filter on a value

        Parameters
        ----------
        filter : str
            The name of the filter (e.g. 'eq' or 'lte')
        other : object
            The value to filter on

        """
        other = self.ddb_dump_for_query(other)
        if other is None and filter in ('ne', 'eq'):
            # Don't bother checking allowed filters because this turns into the
            # "exists" filter, which can be done on anything
            pass
        elif self.overflow:
            # Don't bother checking validity if this is an overflow field
            pass
        elif filter not in self.data_type.allowed_filters:
            raise TypeError("Cannot use '%s' filter on '%s' field" %
                            (filter, self.data_type))
        return Condition.construct(self.name, filter, other)

    def __eq__(self, other):
        return self._make_condition('eq', other)

    def __ne__(self, other):
        return self._make_condition('ne', other)

    def __lt__(self, other):
        return self._make_condition('lt', other)

    def __le__(self, other):
        return self._make_condition('lte', other)

    def __gt__(self, other):
        return self._make_condition('gt', other)

    def __ge__(self, other):
        return self._make_condition('gte', other)

    def _make_contains_condition(self, filter, other):
        """ Construct a query condition for 'contains' or 'ncontains' """
        if not self.overflow and filter not in self.data_type.allowed_filters:
            raise TypeError("Cannot use '%s' filter on '%s' field" %
                            (filter, self.data_type))
        return Condition.construct(self.name, filter,
                                   self.data_type.ddb_dump_inner(other))

    def contains_(self, other):
        """ Create a query condition that this field must contain a value """
        return self._make_contains_condition('contains', other)

    def ncontains_(self, other):
        """ Create a query condition that this field cannot contain a value """
        return self._make_contains_condition('ncontains', other)

    def in_(self, other):
        """
        Create a query condition that this field must be within a set of values

        """
        if self.overflow:
            other = set([self.ddb_dump_overflow(val) for val in other])
        elif 'in' not in self.data_type.allowed_filters:
            raise TypeError("Cannot use '%s' filter on '%s' field" %
                            (filter, self.data_type))
        else:
            other = set([self.ddb_dump_for_query(val) for val in other])
        return Condition.construct(self.name, 'in', other)

    def beginswith_(self, other):
        """
        Create a query condition that this field must begin with a string

        """
        if (not self.overflow and
                'beginswith' not in self.data_type.allowed_filters):
            raise TypeError("Cannot use 'beginswith' filter on '%s' field" %
                            self.data_type)
        if self.overflow:
            # Since strings are dumped to json in overflow fields, we should
            # prefix it with a double quote
            other = '"' + other
        else:
            other = self.ddb_dump_for_query(other)
        return Condition.construct(self.name, 'beginswith', other)

    def between_(self, low, high):
        """
        Create a query condition that this field must be between two values
        (inclusive)

        """
        if (not self.overflow and
                'between' not in self.data_type.allowed_filters):
            raise TypeError("Cannot use 'between' filter on %s field" %
                            self.data_type)
        low = self.ddb_dump_for_query(low)
        high = self.ddb_dump_for_query(high)
        return Condition.construct(self.name, 'between', (low, high))

    def betwixt_(self, low, high):
        """ Poetic version of :meth:`~.between_` """
        return self.between_(low, high)


class Composite(Field):

    """
    A field that is composed of multiple other fields

    Parameters
    ----------
    *fields : list
        List of names of fields that compose this composite field
    hash_key : bool, optional
        This key is a DynamoDB hash key (default False)
    range_key : bool, optional
        This key is a DynamoDB range key (default False)
    index : str, optional
        If present, create a local secondary index on this field with this as
        the name.
    data_type : str, optional
        The dynamo data type. Valid values are (NUMBER, STRING, BINARY,
        NUMBER_SET, STRING_SET, BINARY_SET, dict, list, bool, str, unicode,
        int, float, set, datetime, date, Decimal) (default unicode)
    coerce : bool, optional
        Attempt to coerce the value if it's the incorrect type (default False)
    check : callable, optional
        A function that takes the value and returns True if the value is valid
        (default None)
    merge : callable, optional
        The function that merges the subfields together. By default it simply
        joins them with a ':'.

    """

    def __init__(self, *args, **kwargs):
        self.merge = kwargs.pop('merge', None)
        if self.merge is None:
            self.merge = lambda *args: ':'.join(args)
        unrecognized = (set(kwargs.keys()) -
                        set(['range_key', 'index', 'hash_key', 'data_type',
                             'check', 'coerce']))
        if unrecognized:
            raise TypeError("Unrecognized keyword args: %s" % unrecognized)
        if len(args) < 1:
            raise TypeError("Composite must depend on at least one field")
        super(Composite, self).__init__(**kwargs)
        self.composite = True
        self.subfields = args

    def __contains__(self, key):
        return key in self.subfields

    def resolve(self, obj=None, scope=None):
        """ Resolve a field value from an object or scope dict """
        if scope is not None and self.name in scope:
            return super(Composite, self).resolve(obj, scope)
        args = [self.model.meta_.fields[f].resolve(obj, scope) for f in
                self.subfields]
        return self.coerce(self.merge(*args))

    def get_cached_value(self, obj):
        args = [self.model.meta_.fields[f].get_cached_value(obj) for f in
                self.subfields]
        return self.coerce(self.merge(*args))
