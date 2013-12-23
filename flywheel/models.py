""" Model code """
import time

import boto.dynamodb.types
import contextlib
import copy
import inspect
from boto.dynamodb2.fields import HashKey, RangeKey, BaseSchemaField
from boto.dynamodb2.table import Table
from boto.exception import JSONResponseError
from collections import defaultdict
from decimal import Inexact, Rounded, Decimal

from .fields import Field, NUMBER


# HACK to force conversion of floats to Decimals
boto.dynamodb.types.DYNAMODB_CONTEXT.traps[Inexact] = False
boto.dynamodb.types.DYNAMODB_CONTEXT.traps[Rounded] = False


def float_to_decimal(f):
    """ Monkey-patched replacement for boto's broken version """
    n, d = f.as_integer_ratio()
    numerator, denominator = Decimal(n), Decimal(d)
    ctx = boto.dynamodb.types.DYNAMODB_CONTEXT
    return ctx.divide(numerator, denominator)

boto.dynamodb.types.float_to_decimal = float_to_decimal


class ValidationError(Exception):

    """ Model inconsistency """
    pass


class Ordering(object):

    """ A way that the models are ordered """

    def __init__(self, meta, hash_key, range_key=None, index_name=None):
        self.meta = meta
        self.hash_key = hash_key
        self.range_key = range_key
        self.index_name = index_name

    def score(self, obj=None):
        """ Get the 'score' value for an object """
        return self.range_key.resolve(obj)

    def query_kwargs(self, **kwargs):
        """ Get the boto query kwargs for querying against a field """
        kwargs = {'%s__eq' % self.hash_key.name:
                  self.hash_key.resolve(scope=kwargs)}
        if self.index_name is not None:
            kwargs['index'] = self.index_name
        return kwargs

    def __repr__(self):
        if self.range_key is None:
            return "Ordering(%s, None, %s)" % (self.hash_key.name,
                                               self.index_name)
        else:
            return "Ordering(%s, %s, %s)" % (self.hash_key.name,
                                             self.range_key.name,
                                             self.index_name)


def merge_metadata(cls):
    """ Merge all the __metadata__ dicts in a class's hierarchy """
    cls_meta = getattr(cls, '__metadata__', {})
    meta = {}
    for base in cls.__bases__:
        meta.update(merge_metadata(base))
    # Don't merge any keys that start with '_'
    for key in meta.keys():
        if key.startswith('_'):
            del meta[key]
    meta.update(cls_meta)
    return meta


class ModelMetaclass(type):

    """ Metaclass for Model objects """
    def __new__(mcs, name, bases, dct):
        cls = super(ModelMetaclass, mcs).__new__(mcs, name, bases, dct)

        cls.__metadata__ = merge_metadata(cls)
        cls.__on_create__()

        cls.meta_ = cls.__metadata_class__(cls)
        cls.meta_.post_create()
        cls.meta_.validate_model()
        cls.meta_.post_validate()

        cls.__after_create__()

        return cls

    def __init__(cls, name, bases, dct):
        super(ModelMetaclass, cls).__init__(name, bases, dct)


class ModelMetadata(object):

    """
    Container for model metadata

    Parameters
    ----------
    model : :class:`.Model`

    Attributes
    ----------
    name : str
        The unique name of the model. This is set by the '_name' field in
        __metadata__. Defaults to the name of the model class.
    namespace : list
        The namespace of this model. Set by the Engine.
    global_indexes : list
        List of global indexes (hash_key, [range_key]) pairs.
    related_fields : dict
        Mapping of field names to set of fields that change when that field
        changes (usually just that field name, but can be more if composite
        fields use it)
    orderings : list
        List of :class:`.Ordering`
    throughput : dict
        Mapping of 'read' and 'write' to the table throughput (default 5, 5)

    """
    __order_class__ = Ordering
    namespace = []

    def __init__(self, model):
        self.model = model
        self._name = model.__name__
        self.global_indexes = []
        self.orderings = []
        self.throughput = {'read': 5, 'write': 5}
        self.__dict__.update(model.__metadata__)
        self.name = self._name
        self.fields = {}
        self.hash_key = None
        self.range_key = None
        self.related_fields = defaultdict(set)
        self.all_global_indexes = set()
        for gindex in self.global_indexes:
            self.all_global_indexes.add(gindex.hash_key)
            if gindex.range_key is not None:
                self.all_global_indexes.add(gindex.range_key)

    def post_create(self):
        """ Create the orderings """
        for name, member in inspect.getmembers(self.model):
            if isinstance(member, Field):
                self.fields[name] = member
                member.name = name
                member.model = self.model
                if member.hash_key:
                    self.hash_key = member
                elif member.range_key:
                    self.range_key = member

        self.orderings.append(self.__order_class__(self, self.hash_key,
                                                   self.range_key))
        for field in self.fields.itervalues():
            if field.index:
                order = self.__order_class__(self, self.hash_key, field,
                                             field.index_name)
                self.orderings.append(order)

        for index in self.global_indexes:
            for key in index:
                if key not in self.fields:
                    raise ValidationError("Model %s global index %s "
                                          "references unknown field '%s'" %
                                          (self.name, index.name, key))
            if index.range_key:
                range_key = self.fields[index.range_key]
            else:
                range_key = None
            self.orderings.append(self.__order_class__(self,
                                                       self.fields[
                                                           index.hash_key],
                                                       range_key, index.name))

    def post_validate(self):
        """ Build the dict of related fields """
        def update_related(field, name):
            """ Recursively add a field to related """
            for f in field.subfields:
                self.related_fields[f].add(name)
                subfield = self.fields[f]
                if subfield.composite:
                    update_related(subfield, name)

        for field in self.fields.itervalues():
            self.related_fields[field.name].add(field.name)
            if field.composite:
                update_related(field, field.name)

    def get_ordering_from_fields(self, eq_fields, fields):
        """
        Get a unique ordering from constraint fields

        Parameters
        ----------
        eq_fields : list
        fields : list

        Returns
        -------
        ordering : :class:`.Ordering`

        Raises
        ------
        exc : :class:`TypeError`
            If more than one possible Ordering is found

        """
        orderings = []
        for order in self.orderings:
            needed = order.hash_key.can_resolve(eq_fields)
            if len(needed) == 0:
                continue

            # If there are no non-equality fields, range key must be in the
            # eq_fields
            if len(fields) == 0:
                rng_fields = set(eq_fields)
                rng_needed = order.range_key.can_resolve(rng_fields)
                # hash and range key must use all eq_fields
                if len(set(eq_fields) - needed - rng_needed) != 0:
                    continue
            else:
                # If there are eq_fields left over, continue
                if len(set(eq_fields) - needed) != 0 or len(fields) > 1:
                    continue
                if order.range_key.name not in fields:
                    continue

            orderings.append(order)

        if len(orderings) > 1:
            for order in orderings:
                if order.index_name is None:
                    return order
            raise ValueError("More than one ordering found: %s" % orderings)
        elif len(orderings) == 1:
            return orderings[0]
        else:
            return None

    def get_ordering_from_index(self, index):
        """ Get the ordering with matching index name """
        for order in self.orderings:
            if order.index_name == index:
                return order
        raise ValueError("Cannot find ordering with index name '%s'" % index)

    def rk(self, obj=None, scope=None):
        """ Construct the range key value """
        if self.range_key is not None:
            return self.range_key.resolve(obj, scope)

    def hk(self, obj=None, scope=None):
        """ Construct the primary key value """
        return self.hash_key.resolve(obj, scope)

    def pk(self, obj=None, scope=None):
        """ Get the concatenated primary key for an item """
        hk = self.hk(obj, scope)
        rk = self.rk(obj, scope)
        if rk is not None:
            return "%s:%s" % (hk, rk)
        else:
            return hk

    @property
    def ddb_tablename(self):
        """ The name of the DynamoDB table """
        return '-'.join(self.namespace + [self.name])

    def ddb_table(self, connection):
        """ Construct a Dynamo table from a connection """
        return Table(self.ddb_tablename, connection=connection)

    def validate_model(self):
        """ Perform validation checks on the model declaration """
        if self.model.__dict__.get('__abstract__'):
            return
        hash_keys = [f for f in self.fields.values() if f.hash_key]
        range_keys = [f for f in self.fields.values() if f.range_key]
        indexes = [f for f in self.fields.values() if f.index]
        name = self.name
        if len(hash_keys) != 1:
            raise ValidationError("Model %s must have exactly one hash key" %
                                  name)
        if len(range_keys) > 1:
            raise ValidationError("Model %s can't have more than one range key"
                                  % name)
        if len(indexes) > 5:
            raise ValidationError("Model %s can't have more than 5 "
                                  "local indexes" % name)
        if len(indexes) > 0 and len(range_keys) == 0:
            raise ValidationError("Model %s can't set indexes without a "
                                  "range key" % name)
        if len(self.global_indexes) > 5:
            raise ValidationError("Model %s can't have more than 5 "
                                  "global indexes" % name)
        for field in self.fields.values():
            if field.composite:
                for f in field.subfields:
                    if f not in self.fields:
                        raise ValidationError("Model %s key %s references "
                                              "unknown field '%s'" %
                                              (name, field.name, f))
                    if f == field.name:
                        raise ValidationError("Model %s key %s cannot contain "
                                              "itself" % (name, field.name))

    def create_dynamo_schema(self, connection, tablenames=None, test=False,
                             wait=False, throughput=None):
        """
        Create all Dynamo tables for this model

        Parameters
        ----------
        connection : :class:`~boto.dynamodb2.layer1.DynamoDBConnection`
        tablenames : list, optional
            List of tables that already exist. Will call 'describe' if not
            provided.
        test : bool, optional
            If True, don't actually create the table (default False)
        wait : bool, optional
            If True, block until table has been created (default False)
        throughput : dict, optional
            The throughput of the table and global indexes. Has the keys 'read'
            and 'write'. To specify throughput for global indexes, add the name
            of the index as a key and another 'read', 'write' dict as the
            value.

        Returns
        -------
        table : str
            Table name that was created, or None if nothing created

        """
        if tablenames is None:
            tablenames = connection.list_tables()['TableNames']
        if self.ddb_tablename in tablenames:
            return None
        elif test:
            return self.ddb_tablename

        attrs = []
        indexes = []
        global_indexes = []
        hash_key = None
        raw_attrs = {}

        if throughput is not None:
            table_throughput = throughput
        else:
            table_throughput = self.throughput
        table_throughput = {
            'ReadCapacityUnits': table_throughput['read'],
            'WriteCapacityUnits': table_throughput['write'],
        }

        hash_key = HashKey(self.hash_key.name,
                           data_type=self.hash_key.ddb_data_type)
        schema = [hash_key.schema()]
        for name, field in self.fields.iteritems():
            if field.hash_key:
                f = hash_key
            elif field.range_key:
                f = RangeKey(name, data_type=field.ddb_data_type)
                schema.append(f.schema())
            elif field.index:
                idx = field.get_boto_index(hash_key)
                f = idx.parts[1]
                indexes.append(idx.schema())
            elif any(map(lambda x: name in x, self.global_indexes)):
                f = BaseSchemaField(name, data_type=field.ddb_data_type)
            else:
                continue
            attrs.append(f.definition())
            raw_attrs[name] = f

        for gindex in self.global_indexes:
            index = gindex.get_boto_index(self.fields)
            if throughput is not None and gindex.name in throughput:
                index.throughput = throughput[gindex.name]
            global_indexes.append(index.schema())

        # Make sure indexes & global indexes either have data or are None
        indexes = indexes or None
        global_indexes = global_indexes or None
        if not test:
            connection.create_table(
                attrs, self.ddb_tablename, schema, table_throughput,
                local_secondary_indexes=indexes,
                global_secondary_indexes=global_indexes)
            if wait:
                desc = connection.describe_table(self.ddb_tablename)
                while desc['Table']['TableStatus'] != 'ACTIVE':
                    time.sleep(1)
                    desc = connection.describe_table(self.ddb_tablename)

        return self.ddb_tablename

    def delete_dynamo_schema(self, connection, tablenames=None, test=False,
                             wait=False):
        """
        Drop all Dynamo tables for this model

        Parameters
        ----------
        connection : :class:`~boto.dynamodb2.layer1.DynamoDBConnection`
        tablenames : list, optional
            List of tables that already exist. Will call 'describe' if not
            provided.
        test : bool, optional
            If True, don't actually delete the table (default False)
        wait : bool, optional
            If True, block until table has been deleted (default False)

        Returns
        -------
        table : str
            Table name that was deleted, or None if nothing deleted

        """
        if tablenames is None:
            tablenames = connection.list_tables()['TableNames']

        if self.ddb_tablename in tablenames:
            if not test:
                self.ddb_table(connection).delete()
                if wait:
                    try:
                        connection.describe_table(self.ddb_tablename)
                        while True:
                            time.sleep(1)
                            connection.describe_table(self.ddb_tablename)
                    except JSONResponseError as e:
                        if e.status != 400:
                            raise
            return self.ddb_tablename
        return None


class Model(object):

    """
    Base class for all tube models

    For documentation on the metadata fields, check the attributes on the
    :class:`.ModelMetadata` class.

    """
    __abstract__ = True
    __metaclass__ = ModelMetaclass
    __metadata_class__ = ModelMetadata
    __metadata__ = {}
    meta_ = None
    persisted_ = False
    __engine__ = None
    _overflow = None
    __dirty__ = None
    __cache__ = None
    __incrs__ = None
    _loading = False

    @classmethod
    def __on_create__(cls):
        """ Called after class is constructed but before meta_ is set """
        pass

    @classmethod
    def __after_create__(cls):
        """ Called after class is constructed but before meta_ is set """
        pass

    @classmethod
    def field_(cls, name):
        """ Construct a placeholder Field for an undeclared field """
        field = Field()
        field.name = name
        field.overflow = True
        return field

    def __new__(cls, *_, **__):
        """ Override __new__ to set default field values """
        obj = super(Model, cls).__new__(cls)
        with obj.loading():
            for name, field in cls.meta_.fields.iteritems():
                if not field.composite:
                    setattr(obj, name, field.default)
        obj._overflow = {}
        obj.persisted_ = False
        return obj

    def __setattr__(self, name, value):
        if name.startswith('_') or name.endswith('_'):
            # Don't interfere with private fields
            super(Model, self).__setattr__(name, value)
            return
        if self.persisted_:
            if ((self.meta_.hash_key.name in self.meta_.related_fields[name])
                    or (self.meta_.range_key is not None and
                        self.meta_.range_key.name in
                        self.meta_.related_fields[name])):
                if value != getattr(self, name):
                    raise AttributeError(
                        "Cannot change an item's primary key!")
                else:
                    return
        self.mark_dirty_(name)
        field = self.meta_.fields.get(name)
        if field is not None:
            # Ignore if trying to set a composite field
            if not field.composite:
                if (not self._loading and self.persisted_ and
                        name not in self.__cache__):
                    for related in self.meta_.related_fields[name]:
                        cached_var = copy.copy(getattr(self, related))
                        self.__cache__[related] = cached_var
                super(Model, self).__setattr__(name, field.coerce(value))
        else:
            self._overflow[name] = value

    def __delattr__(self, name):
        if name.startswith('_') or name.endswith('_'):
            # Don't interfere with private fields
            super(Model, self).__delattr__(name)
            return
        field = self.meta_.fields.get(name)
        if field is not None:
            if not field.composite:
                super(Model, self).__delattr__(name)
        else:
            del self._overflow[name]

    def __getattribute__(self, name):
        if not name.startswith('_') and not name.endswith('_'):
            field = self.meta_.fields.get(name)
            # Intercept getattribute to construct composite fields on the fly
            if field is not None and field.composite:
                return field.resolve(self)
        return super(Model, self).__getattribute__(name)

    def __getattr__(self, name):
        try:
            return self._overflow[name]
        except KeyError:
            raise AttributeError("%s not found" % name)

    def mark_dirty_(self, name):
        """ Mark that a field is dirty """
        if self._loading or self.__dirty__ is None:
            return
        if name in self.__incrs__:
            raise ValueError("Cannot increment field '%s' and set it in "
                             "the same update!" % name)
        if name in self.meta_.fields:
            self.__dirty__.update(self.meta_.related_fields[name])
            # Never mark the primary key as dirty
            if self.meta_.hash_key.name in self.__dirty__:
                self.__dirty__.remove(self.meta_.hash_key.name)
            if (self.meta_.range_key is not None and
                    self.meta_.range_key.name in self.__dirty__):
                self.__dirty__.remove(self.meta_.range_key.name)
        else:
            self.__dirty__.add(name)

    def get(self, name, default=None):
        """ Dict-style getter for overflow attrs """
        return self._overflow.get(name, default)

    @property
    def hk_(self):
        """ The value of the hash key """
        return self.meta_.hk(self)

    @property
    def rk_(self):
        """ The value of the range key """
        return self.meta_.rk(self)

    @property
    def pk_dict_(self):
        """ The primary key dict """
        pk = {self.meta_.hash_key.name: self.hk_}
        if self.meta_.range_key is not None:
            pk[self.meta_.range_key.name] = self.rk_
        return pk

    def keys_(self):
        """ All declared fields and any additional fields """
        return self.meta_.fields.keys() + self._overflow.keys()

    def cached_(self, name):
        """ Get the cached (server) value of a field """
        if not self.persisted_:
            return None
        if name in self.__cache__:
            return self.__cache__[name]
        return getattr(self, name, None)

    def incr_(self, **kwargs):
        """ Atomically increment a number value """
        for key, val in kwargs.iteritems():
            if ((self.meta_.hash_key.name in self.meta_.related_fields[key])
                    or (self.meta_.range_key is not None and
                        self.meta_.range_key.name in
                        self.meta_.related_fields[key])):
                raise AttributeError("Cannot increment an item's primary key!")

            field = self.meta_.fields.get(key)
            if field is not None:
                if field.data_type != NUMBER:
                    raise ValueError("Cannot increment non-number field '%s'" %
                                     key)
                if field.composite:
                    raise ValueError("Cannot increment composite field '%s'" %
                                     key)
            if key in self.__dirty__:
                raise ValueError("Cannot set field '%s' and increment it in "
                                 "the same update!" % key)
            self.__incrs__[key] = getattr(self, key, 0) + val
            if field is not None:
                for name in self.meta_.related_fields[key]:
                    self.__cache__[name] = getattr(self, key)
                    if name != key:
                        self.__dirty__.add(name)
                self.__dict__[key] = self.__incrs__[key]
            else:
                self.__cache__[key] = getattr(self, key, 0)
                self._overflow[key] = self.__incrs__[key]

    def pre_save(self, engine):
        """ Called before saving items """
        self.__engine__ = engine

    def post_save(self):
        """ Called after item is saved to database """
        self.persisted_ = True
        self.__dirty__ = set()
        self.__incrs__ = {}
        self._reset_cache()

    def refresh(self, consistent=False):
        """ Overwrite model data with freshest from database """
        if self.__engine__ is None:
            raise ValueError("Cannot sync: No DB connection")

        self.__engine__.refresh(self, consistent=consistent)

    def sync(self, atomic=False):
        """ Sync model changes back to database """
        if self.__engine__ is None:
            raise ValueError("Cannot sync: No DB connection")

        self.__engine__.sync(self, atomic=atomic)

    def delete(self, atomic=False):
        """ Delete the model from the database """
        if self.__engine__ is None:
            raise ValueError("Cannot delete: No DB connection")
        self.__engine__.delete(self, atomic=atomic)

    def post_load(self, engine):
        """ Called after model loaded from database """
        if engine is not None:
            self.__engine__ = engine
        self.persisted_ = True
        self.__dirty__ = set()
        self.__incrs__ = {}
        self._reset_cache()

    def _reset_cache(self):
        """ Reset the __cache__ to only track mutable fields """
        self.__cache__ = {}
        for name, field in self.meta_.fields.iteritems():
            if field.is_mutable:
                self.__cache__[name] = copy.copy(getattr(self, name))

    @contextlib.contextmanager
    def loading(self, engine=None):
        """ Context manager to speed up object load process """
        self._loading = True
        self._overflow = {}
        yield
        self._loading = False
        self.post_load(engine)

    def ddb_dump_field(self, name):
        """ Dump a field to a Dynamo-friendly value """
        val = getattr(self, name)
        if name in self.meta_.fields:
            return self.meta_.fields[name].ddb_dump(val)
        else:
            return Field.ddb_dump_overflow(val)

    def ddb_dump(self):
        """ Return a dict for inserting into DynamoDB """
        data = {}
        for name in self.meta_.fields:
            data[name] = self.ddb_dump_field(name)
        for name in self._overflow:
            data[name] = self.ddb_dump_field(name)

        return data

    def set_ddb_val(self, key, val):
        """ Decode and set a value retrieved from Dynamo """
        if key.startswith('_'):
            pass
        elif key in self.meta_.fields:
            setattr(self, key, self.meta_.fields[key].ddb_load(val))
        else:
            setattr(self, key, Field.ddb_load_overflow(val))

    @classmethod
    def ddb_load(cls, engine, data):
        """ Load a model from DynamoDB data """
        obj = cls.__new__(cls)
        with obj.loading(engine):
            for key, val in data.items():
                obj.set_ddb_val(key, val)
        return obj

    def __json__(self, request=None):
        data = {}
        for name in self.meta_.fields:
            data[name] = getattr(self, name)
        for key, val in self._overflow.iteritems():
            data[key] = val
        return data

    def __hash__(self):
        return hash(self.hk_) + hash(self.rk_)

    def __eq__(self, other):
        return (self.meta_.name == other.meta_.name and self.hk_ == other.hk_
                and self.rk_ == other.rk_)

    def __ne__(self, other):
        return not self.__eq__(other)
