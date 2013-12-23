""" Field declarations for models """
from datetime import datetime, date

import json
from boto.dynamodb.types import Binary
from boto.dynamodb2.fields import (HashKey, RangeKey, AllIndex, KeysOnlyIndex,
                                   IncludeIndex, GlobalAllIndex,
                                   GlobalKeysOnlyIndex, GlobalIncludeIndex)
from boto.dynamodb2.types import (NUMBER, STRING, BINARY, NUMBER_SET,
                                  STRING_SET, BINARY_SET)
from decimal import Decimal


class GlobalIndex(object):

    """
    A global index for DynamoDB

    Parameters
    ----------
    name : str
        The name of the index
    hash_key : str
        The name of the field that is the hash key for the index
    range_key : str, optional
        The name of the field that is the range key for the index
    throughput : dict, optional
        The read/write throughput of this global index. Used when creating a
        table. Dict has a 'read' and a 'write' key. (Default 5, 5)

    """

    def __init__(self, name, hash_key, range_key=None):
        self.name = name
        self.hash_key = hash_key
        self.range_key = range_key
        self._throughput = {'read': 5, 'write': 5}
        self.boto_index = GlobalAllIndex
        self.kwargs = {}

    @classmethod
    def all(cls, name, hash_key, range_key=None):
        """ Project all attributes into the index """
        return cls(name, hash_key, range_key)

    @classmethod
    def keys(cls, name, hash_key, range_key=None):
        """ Project key attributes into the index """
        index = cls(name, hash_key, range_key)
        index.boto_index = GlobalKeysOnlyIndex
        return index

    @classmethod
    def include(cls, name, hash_key, range_key=None, includes=None):
        """ Select which attributes to project into the index """
        includes = includes or []
        index = cls(name, hash_key, range_key)
        index.boto_index = GlobalIncludeIndex
        index.kwargs['includes'] = includes
        return index

    def get_boto_index(self, fields):
        """ Get the boto index class for this GlobalIndex """
        hash_key = HashKey(self.hash_key,
                           data_type=fields[self.hash_key].ddb_data_type)
        parts = [hash_key]
        if self.range_key is not None:
            range_key = RangeKey(self.range_key,
                                 data_type=fields[self.range_key].ddb_data_type)
            parts.append(range_key)
        index = self.boto_index(self.name, parts, **self.kwargs)
        # Throughput has to be patched on afterwards due to bug in
        # GlobalIncludeIndex
        index.throughput = self._throughput
        return index

    def get_throughput(self):
        """ Get the provisioned throughput """
        return self._throughput

    def throughput(self, read=5, write=5):
        """
        Set the index throughput

        Parameters
        ----------
        read : int, optional
            Amount of read throughput (default 5)
        write : int, optional
            Amount of write throughput (default 5)

        Notes
        -----
        This is meant to be used as a chain::

            class MyModel(Model):
                __metadata__ = {
                    'global_indexes': [
                        GlobalIndex('myindex', 'hkey', 'rkey').throughput(5, 2)
                    ]
                }

        """
        self._throughput = {
            'read': read,
            'write': write,
        }
        return self

    def __contains__(self, field):
        return field == self.hash_key or field == self.range_key

    def __iter__(self):
        yield self.hash_key
        if self.range_key is not None:
            yield self.range_key

    def __hash__(self):
        return hash(self.name)

    def __eq__(self, other):
        return (self.name == other.name and self.hash_key == other.hash_key and
                self.range_key == other.range_key)

    def __ne__(self, other):
        return not self.__eq__(other)

    def __repr__(self):
        if self.range_key is None:
            return "GlobalIndex('%s', '%s')" % (self.name, self.hash_key)
        else:
            return "GlobalIndex('%s', '%s', '%s')" % (self.name, self.hash_key,
                                                      self.range_key)

    def __str__(self):
        return repr(self)


class Condition(object):

    """


    Attributes
    ----------
    eq_fields : dict
        Mapping of field name to field value
    fields : dict
        Mapping of field name to (operator, value) tuples
    limit : int
        Maximum number of results
    index_name : str
        Name of index to use for a query

    """

    def __init__(self):
        self.eq_fields = {}
        self.fields = {}
        self.limit = None
        self.index_name = None

    def scan_kwargs(self):
        """ Key the kwargs for doing a table scan """
        kwargs = {}
        for key, val in self.eq_fields.iteritems():
            kwargs["%s__eq" % key] = val
        for key, (op, val) in self.fields.iteritems():
            kwargs["%s__%s" % (key, op)] = val
        if self.limit is not None:
            kwargs['limit'] = self.limit
        return kwargs

    def query_kwargs(self, model):
        """ Get the kwargs for doing a table query """
        scan_only = set(['contains', 'ncontains', 'null', 'in', 'ne'])
        for op, _ in self.fields.itervalues():
            if op in scan_only:
                raise ValueError("Operation '%s' cannot be used in a query!" %
                                 op)
        if self.index_name is not None:
            ordering = model.meta_.get_ordering_from_index(self.index_name)
        else:
            ordering = model.meta_.get_ordering_from_fields(
                self.eq_fields.keys(),
                self.fields.keys())

        if ordering is None:
            raise ValueError("Bad query arguments. You must provide a hash key "
                             "and may optionally constrain on exactly one "
                             "range key")
        kwargs = ordering.query_kwargs(**self.eq_fields)
        if ordering.range_key is not None:
            if len(self.fields) > 0:
                key, (op, val) = self.fields.items()[0]
                kwargs['%s__%s' % (key, op)] = val
            else:
                try:
                    key = '%s__eq' % ordering.range_key.name
                    val = ordering.range_key.resolve(scope=self.eq_fields)
                    kwargs[key] = val
                except KeyError:
                    # No range key constraint in query
                    pass

        if self.limit is not None:
            kwargs['limit'] = self.limit
        return kwargs

    @classmethod
    def construct(cls, field, op, other):
        """
        Create a Condition on a field

        Parameters
        ----------
        field : str
            Name of the field to constrain
        op : str
            Operator, such as 'eq', 'lt', or 'contains'
        other : object
            The value to constrain the field with

        Returns
        -------
        condition : :class:`.Condition`

        """
        c = cls()
        if other is None:
            if op == 'eq':
                c.fields[field] = ('null', True)
            elif op == 'ne':
                c.fields[field] = ('null', False)
            else:
                raise ValueError("Cannot filter %s None" % op)
        elif op == 'eq':
            c.eq_fields[field] = other
        else:
            c.fields[field] = (op, other)
        return c

    @classmethod
    def construct_limit(cls, count):
        """
        Create a condition that will limit the results to a count

        Parameters
        ----------
        count : int

        Returns
        -------
        condition : :class:`.Condition`

        """
        c = cls()
        c.limit = count
        return c

    @classmethod
    def construct_index(cls, name):
        """
        Force the query to use a certain index

        Parameters
        ----------
        name : str

        Returns
        -------
        condition : :class:`.Condition`

        """
        c = cls()
        c.index_name = name
        return c

    def __and__(self, other):
        new_condition = Condition()
        new_condition.eq_fields.update(self.eq_fields)
        new_condition.fields.update(self.fields)
        new_condition.eq_fields.update(other.eq_fields)
        new_condition.fields.update(other.fields)
        if self.limit and other.limit:
            raise ValueError("Trying to combine two conditions with a "
                             "'limit' constraint!")
        new_condition.limit = self.limit or other.limit
        if self.index_name and other.index_name:
            raise ValueError("Trying to combine two conditions with an "
                             "'index' constraint!")
        new_condition.index_name = self.index_name or other.index_name
        return new_condition


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
        If present, this key is indexed in DynamoDB. This field is the name of
        the index.
    data_type : str, optional
        The dynamo data type. Valid values are (NUMBER, STRING, BINARY,
        NUMBER_SET, STRING_SET, BINARY_SET, dict, bool) (default STRING)
    coerce : bool, optional
        Attempt to coerce the value if it's the incorrect type (default False)
    nullable : bool, optional
        If True, the field is not required (default False)
    check : callable, optional
        A function that takes the value and returns True if the value is valid
        (default None)

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
                 data_type=unicode, coerce=False, nullable=True, check=None):
        if sum((hash_key, range_key)) > 1:
            raise ValueError("hash_key and range_key are mutually exclusive!")
        if data_type not in (STRING, NUMBER, BINARY, STRING_SET, NUMBER_SET,
                             BINARY_SET, dict, bool, list, datetime, str,
                             unicode, int, float, set, date, Decimal):
            raise TypeError("Unknown data type '%s'" % data_type)
        self.name = None
        self.model = None
        self.composite = False
        self.overflow = False
        self.data_type = data_type
        self._coerce = coerce
        self.nullable = nullable
        self.check = check
        self.hash_key = hash_key
        self.range_key = range_key
        self.subfields = []
        self.index = False
        self.index_name = None
        self._boto_index = None
        self._boto_index_kwargs = None
        if index:
            self.all_index(index)
        if hash_key or range_key:
            self.nullable = False

    def get_boto_index(self, hash_key):
        """ Construct a boto index object from a hash key """
        parts = [hash_key, RangeKey(self.name, data_type=self.ddb_data_type)]
        return self._boto_index(self.index_name, parts=parts,
                                **self._boto_index_kwargs)

    def _set_boto_index(self, name, boto_index, **kwargs):
        """ Set the type of index """
        if self.hash_key or self.range_key:
            raise ValueError("Cannot index the hash or range key!")
        if self.index:
            raise ValueError("Index is already set!")
        self.index_name = name
        self._boto_index = boto_index
        self._boto_index_kwargs = kwargs
        self.index = True

    def all_index(self, name):
        """
        Index this field and project all attributes

        Parameters
        ----------
        name : str
            The name of the index

        """
        self._set_boto_index(name, AllIndex)
        return self

    def keys_index(self, name):
        """
        Index this field and project all key attributes

        Parameters
        ----------
        name : str
            The name of the index

        """
        self._set_boto_index(name, KeysOnlyIndex)
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
        self._set_boto_index(name, IncludeIndex, includes=includes)
        return self

    def coerce(self, value, force_coerce=None):
        """ Coerce the value to the field's data type """
        if value is None:
            return value
        if force_coerce is None:
            force_coerce = self._coerce
        if self.data_type in (STRING, unicode):
            if not isinstance(value, unicode):
                # Silently convert str to unicode using utf-8
                if isinstance(value, str):
                    return value.decode('utf-8')
                if force_coerce:
                    return unicode(value)
                else:
                    raise TypeError("Field '%s' must be a unicode string! %s" %
                                    (self.name, repr(value)))
        elif self.data_type == str:
            if not isinstance(value, str):
                # Silently convert unicode to str using utf-8
                if isinstance(value, unicode):
                    return value.encode('utf-8')
                if force_coerce:
                    return str(value)
                else:
                    raise TypeError("Field '%s' must be a byte string! %s" %
                                    (self.name, repr(value)))
        elif self.data_type == int:
            if not isinstance(value, int):
                if force_coerce:
                    return Decimal(value)
                else:
                    raise TypeError("Field '%s' must be an int! %s" %
                                    (self.name, repr(value)))
        elif self.data_type in (NUMBER, float):
            if not (isinstance(value, int) or isinstance(value, float) or
                    isinstance(value, Decimal)):
                if force_coerce:
                    return Decimal(value)
                else:
                    raise TypeError("Field '%s' must be a number! %s" %
                                    (self.name, repr(value)))
        elif self.data_type == Decimal:
            if not isinstance(value, Decimal):
                if force_coerce:
                    return Decimal(value)
                else:
                    import traceback
                    traceback.print_stack()
                    return TypeError("Field '%s' must be a Decimal! %s" %
                                     (self.name, repr(value)))
        elif self.data_type == BINARY:
            if not isinstance(value, Binary):
                if force_coerce:
                    return Binary(value)
                else:
                    raise TypeError("Field '%s' must be a Binary! %s" %
                                    (self.name, repr(value)))
        elif self.data_type in (STRING_SET, NUMBER_SET, BINARY_SET, set):
            if not isinstance(value, set):
                if force_coerce:
                    return set(value)
                else:
                    raise TypeError("Field '%s' must be a set! %s" %
                                    (self.name, repr(value)))
        elif self.data_type == dict:
            if not isinstance(value, dict):
                if force_coerce:
                    return dict(value)
                else:
                    raise TypeError("Field '%s' must be a dict! %s" %
                                    (self.name, repr(value)))
        elif self.data_type == list:
            if not isinstance(value, list):
                if force_coerce:
                    return list(value)
                else:
                    raise TypeError("Field '%s' must be a list! %s" %
                                    (self.name, repr(value)))
        elif self.data_type == bool:
            if not isinstance(value, bool):
                if force_coerce:
                    return bool(value)
                else:
                    raise TypeError("Field '%s' must be a bool! %s" %
                                    (self.name, repr(value)))
        elif self.data_type == datetime:
            if not isinstance(value, datetime):
                raise TypeError("Field '%s' must be a datetime! %s" %
                                (self.name, repr(value)))
        elif self.data_type == date:
            if not isinstance(value, date):
                raise TypeError("Field '%s' must be a date! %s" %
                                (self.name, repr(value)))
        return value

    @property
    def is_mutable(self):
        """ Return True if the data type is a set """
        return self.data_type in (STRING_SET, NUMBER_SET, BINARY_SET, dict,
                                  list, set)

    def ddb_dump(self, value):
        """ Dump a value to its Dynamo format """
        if value is None:
            return None
        if self.data_type in (dict, list):
            return json.dumps(value)
        elif self.data_type == bool:
            return int(value)
        elif self.data_type == datetime:
            return float(value.strftime('%s.%f'))
        elif self.data_type == date:
            return int(value.strftime('%s'))
        elif self.data_type == str:
            return value.decode('utf-8')
        return value

    def ddb_dump_for_query(self, value):
        """ Dump a value to format for use in a Dynamo query """
        if value is None:
            return None
        if self.overflow:
            return self.ddb_dump_overflow(value)
        value = self.coerce(value, force_coerce=True)
        if self.data_type in (dict, list):
            raise TypeError("Cannot query on %s objects!" % self.data_type)
        elif self.data_type == bool:
            return int(value)
        elif self.data_type == datetime:
            return float(value.strftime('%s.%f'))
        elif self.data_type == date:
            return int(value.strftime('%s'))
        elif self.data_type == str:
            return value.decode('utf-8')
        else:
            return value

    def ddb_load(self, val):
        """ Decode a value retrieved from Dynamo """
        if self.data_type != Decimal and isinstance(val, Decimal):
            if val % 1 == 0:
                val = int(val)
            else:
                val = float(val)
        if self.data_type in (dict, list):
            return json.loads(val)
        elif self.data_type == bool:
            return bool(val)
        elif self.data_type == datetime:
            return datetime.fromtimestamp(val)
        elif self.data_type == date:
            return date.fromtimestamp(val)
        elif self.data_type == str:
            return val.encode('utf-8')
        return val

    @classmethod
    def ddb_dump_overflow(cls, val):
        """ Dump an overflow value to its Dynamo format """
        if val is None:
            return None
        elif isinstance(val, int) or isinstance(val, float):
            return val
        elif isinstance(val, set):
            return val
        else:
            return json.dumps(val)

    @classmethod
    def ddb_load_overflow(cls, val):
        """ Decode a value of an overflow field """
        if (isinstance(val, Decimal) or isinstance(val, float) or
           isinstance(val, int)):
            return val
        elif isinstance(val, set):
            return val
        else:
            return json.loads(val)

    @property
    def default(self):
        """ The default value for a field of this type """
        if self.data_type in (STRING, BINARY, str, unicode):
            return None
        elif self.data_type in (NUMBER, int, float):
            return 0
        elif self.data_type == Decimal:
            return Decimal('0')
        elif self.data_type in (STRING_SET, NUMBER_SET, BINARY_SET, set):
            return set()
        elif self.data_type == dict:
            return {}
        elif self.data_type == bool:
            return False
        elif self.data_type == list:
            return []

    @property
    def ddb_data_type(self):
        """ Get the DynamoDB data type as used by boto """
        if self.data_type in (int, float, bool, datetime, date, Decimal):
            return NUMBER
        elif self.data_type in (str, unicode, list, dict):
            return STRING
        return self.data_type

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
        if self.name in fields:
            needed.add(self.name)
        elif self.subfields:
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

    def __eq__(self, other):
        other = self.ddb_dump_for_query(other)
        if (other is not None and self.data_type in
                (NUMBER_SET, STRING_SET, BINARY_SET, set)):
            raise TypeError("Cannot use 'equality' filter on set field")
        return Condition.construct(self.name, 'eq', other)

    def __ne__(self, other):
        other = self.ddb_dump_for_query(other)
        if (other is not None and self.data_type in
                (NUMBER_SET, STRING_SET, BINARY_SET, set)):
            raise TypeError("Cannot use 'equality' filter on set field")
        return Condition.construct(self.name, 'ne', other)

    def __lt__(self, other):
        if self.data_type in (bool, NUMBER_SET, STRING_SET, BINARY_SET, set):
            raise TypeError("Cannot use 'inequality' filter on %s field" %
                            self.data_type)
        other = self.ddb_dump_for_query(other)
        return Condition.construct(self.name, 'lt', other)

    def __le__(self, other):
        if self.data_type in (bool, NUMBER_SET, STRING_SET, BINARY_SET, set):
            raise TypeError("Cannot use 'inequality' filter on %s field" %
                            self.data_type)
        other = self.ddb_dump_for_query(other)
        return Condition.construct(self.name, 'lte', other)

    def __gt__(self, other):
        if self.data_type in (bool, NUMBER_SET, STRING_SET, BINARY_SET, set):
            raise TypeError("Cannot use 'inequality' filter on %s field" %
                            self.data_type)
        other = self.ddb_dump_for_query(other)
        return Condition.construct(self.name, 'gt', other)

    def __ge__(self, other):
        if self.data_type in (bool, NUMBER_SET, STRING_SET, BINARY_SET, set):
            raise TypeError("Cannot use 'inequality' filter on %s field" %
                            self.data_type)
        other = self.ddb_dump_for_query(other)
        return Condition.construct(self.name, 'gte', other)

    def contains_(self, other):
        """ Create a query condition that this field must contain a value """
        if (not self.overflow and self.data_type not in
                (NUMBER_SET, STRING_SET, BINARY_SET, set)):
            raise TypeError("Field '%s' is not a set! Cannot use 'contains' "
                            "constraint." % self.name)
        return Condition.construct(self.name, 'contains', other)

    def ncontains_(self, other):
        """ Create a query condition that this field cannot contain a value """
        if (not self.overflow and self.data_type not in
                (NUMBER_SET, STRING_SET, BINARY_SET, set)):
            raise TypeError("Field '%s' is not a set! Cannot use 'ncontains' "
                            "constraint." % self.name)
        return Condition.construct(self.name, 'ncontains', other)

    def in_(self, other):
        """
        Create a query condition that this field must be within a set of values

        """
        if self.overflow:
            other = set([self.ddb_dump_overflow(val) for val in other])
        elif self.data_type in (bool,):
            raise TypeError("Cannot use 'in' filter on %s field" %
                            self.data_type)
        else:
            other = set([self.ddb_dump_for_query(val) for val in other])
        return Condition.construct(self.name, 'in', other)

    def beginswith_(self, other):
        """
        Create a query condition that this field must begin with a string

        """
        if self.overflow:
            other = '"' + other
        elif self.data_type not in (STRING, str, unicode):
            raise TypeError("Field '%s' is not a string! Cannot use "
                            "'beginswith' constraint." % self.name)
        return Condition.construct(self.name, 'beginswith', other)


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
        If present, this key is indexed in DynamoDB. This field is the name of
        the index.
    data_type : str, optional
        The dynamo data type. Valid values are (NUMBER, STRING, BINARY,
        NUMBER_SET, STRING_SET, BINARY_SET, dict, bool) (default STRING)
    nullable : bool, optional
        If True, the field is not required (default True)
    merge : callable, optional
        The function that merges the subfields together. By default it simply
        joins them with a ':'.

    """

    def __init__(self, *args, **kwargs):
        self.merge = kwargs.pop('merge', None)
        if self.merge is None:
            self.merge = lambda *args: ':'.join(args)
        unrecognized = (set(kwargs.keys()) -
                        set(['range_key', 'index', 'hash_key', 'data_type', 'nullable']))
        if unrecognized:
            raise TypeError("Unrecognized keyword args: %s" % unrecognized)
        if len(args) < 2:
            raise TypeError("Composite must consist of two or more fields")
        kwargs.setdefault('nullable', False)
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
        return self.merge(*args)
