""" Field declarations for models """
import json
from boto.dynamodb.types import Binary
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

    def __init__(self, name, hash_key, range_key=None, throughput=None):
        self.name = name
        self.hash_key = hash_key
        self.range_key = range_key
        self.throughput = throughput or {'read': 5, 'write': 5}

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

    """

    def __init__(self, hash_key=False, range_key=False, index=None,
                 data_type=STRING, coerce=False, nullable=True, check=None):
        if sum((hash_key, range_key, bool(index))) > 1:
            raise TypeError("hash_key, range_key, and index are "
                            "mutually exclusive!")
        if data_type not in (STRING, NUMBER, BINARY, STRING_SET, NUMBER_SET,
                             BINARY_SET, dict, bool):
            raise TypeError("Unknown data type '%s'" % data_type)
        self.name = None
        self.model = None
        self.composite = False
        self.overflow = False
        self.data_type = data_type
        self._coerce = coerce
        self.index = index
        self.nullable = nullable
        self.check = check
        self.hash_key = hash_key
        self.range_key = range_key
        self.subfields = []
        if hash_key or range_key:
            self.nullable = False

    def coerce(self, value):
        """ Coerce the value to the field's data type """
        if value is None:
            return value
        if self.data_type == STRING:
            if not isinstance(value, basestring):
                if self._coerce:
                    return str(value)
                else:
                    raise ValueError("Field '%s' must be a string!" %
                                     self.name)
        elif self.data_type == NUMBER:
            if not (isinstance(value, int) or isinstance(value, float) or
                    isinstance(value, Decimal)):
                if self._coerce:
                    return Decimal(value)
                else:
                    raise ValueError("Field '%s' must be a number!" %
                                     self.name)
        elif self.data_type == BINARY:
            if not isinstance(value, Binary):
                if self._coerce:
                    return Binary(value)
                else:
                    raise ValueError("Field '%s' must be a Binary!" %
                                     self.name)
        elif self.data_type in (STRING_SET, NUMBER_SET, BINARY_SET):
            if not isinstance(value, set):
                if self._coerce:
                    return set(value)
                else:
                    raise ValueError("Field '%s' must be a set!" % self.name)
        elif self.data_type == dict:
            if not isinstance(value, dict):
                if not self._coerce:
                    raise ValueError("Field '%s' must be a dict!" % self.name)
        elif self.data_type == bool:
            if not isinstance(value, bool):
                if self._coerce:
                    return bool(value)
                else:
                    raise ValueError("Field '%s' must be a bool!" % self.name)
        return value

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

    @property
    def is_mutable(self):
        """ Return True if the data type is a set """
        return self.data_type in (STRING_SET, NUMBER_SET, BINARY_SET, dict)

    def ddb_dump(self, value):
        """ Dump a value to its Dynamo format """
        if self.data_type == dict:
            return json.dumps(value)
        elif self.data_type == bool:
            return int(value)
        return value

    def ddb_load(self, val):
        """ Decode a value retrieved from Dynamo """
        if isinstance(val, Decimal):
            if val % 1 == 0:
                val = int(val)
            else:
                val = float(val)
        if self.data_type == dict:
            return json.loads(val)
        elif self.data_type == bool:
            return bool(val)
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

    def resolve(self, obj=None, scope=None):
        """ Resolve a field value from an object or scope dict """
        if obj is not None:
            return getattr(obj, self.name)
        else:
            return scope[self.name]

    @property
    def default(self):
        """ The default value for a field of this type """
        if self.data_type in (STRING, BINARY):
            return None
        elif self.data_type == NUMBER:
            return 0
        elif self.data_type in (STRING_SET, NUMBER_SET, BINARY_SET):
            return set()
        elif self.data_type == dict:
            return {}
        elif self.data_type == bool:
            return False

    def __eq__(self, other):
        if self.overflow:
            other = self.ddb_dump_overflow(other)
        return Condition.construct(self.name, 'eq', other)

    def __ne__(self, other):
        if self.overflow:
            other = self.ddb_dump_overflow(other)
        return Condition.construct(self.name, 'ne', other)

    def __lt__(self, other):
        return Condition.construct(self.name, 'lt', other)

    def __le__(self, other):
        return Condition.construct(self.name, 'lte', other)

    def __gt__(self, other):
        return Condition.construct(self.name, 'gt', other)

    def __ge__(self, other):
        return Condition.construct(self.name, 'gte', other)

    def contains_(self, other):
        """ Create a query condition that this field must contain a value """
        if (not self.overflow and
                self.data_type not in (NUMBER_SET, STRING_SET, BINARY_SET)):
            raise TypeError("Field '%s' is not a set! Cannot use 'contains' "
                            "constraint." % self.name)
        return Condition.construct(self.name, 'contains', other)

    def ncontains_(self, other):
        """ Create a query condition that this field cannot contain a value """
        if (not self.overflow and
                self.data_type not in (NUMBER_SET, STRING_SET, BINARY_SET)):
            raise TypeError("Field '%s' is not a set! Cannot use 'ncontains' "
                            "constraint." % self.name)
        return Condition.construct(self.name, 'ncontains', other)

    def in_(self, other):
        """
        Create a query condition that this field must be within a set of values

        """
        if self.overflow:
            other = set([self.ddb_dump_overflow(val) for val in other])
        elif not isinstance(other, set):
            other = set(other)
        return Condition.construct(self.name, 'in', other)

    def beginswith_(self, other):
        """
        Create a query condition that this field must begin with a string

        """
        if self.overflow:
            other = '"' + other
        elif self.data_type != STRING:
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
