""" Model metadata and metaclass objects """
import time

import inspect
from boto.dynamodb2.fields import HashKey, RangeKey, BaseSchemaField
from boto.dynamodb2.table import Table
from boto.exception import JSONResponseError
from collections import defaultdict

from .fields import Field


class ValidationError(Exception):

    """ Model inconsistency """
    pass


class Ordering(object):

    """
    A way that the models are ordered

    This will be a combination of a hash key and a range key. It may be the
    primary key, a local secondary index, or a global secondary index.

    """

    def __init__(self, meta, hash_key, range_key=None, index_name=None):
        self.meta = meta
        self.hash_key = hash_key
        self.range_key = range_key
        self.index_name = index_name

    def query_kwargs(self, **kwargs):
        """ Get the boto query kwargs for querying against this index """
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
    """
    Merge all the __metadata__ dicts in a class's hierarchy

    keys that do not begin with '_' will be inherited.

    keys that begin with '_' will only apply to the object that defines them.

    """
    cls_meta = cls.__dict__.get('__metadata__', {})
    meta = {}
    for base in cls.__bases__:
        meta.update(getattr(base, '__metadata__', {}))
    # Don't merge any keys that start with '_'
    for key in meta.keys():
        if key.startswith('_'):
            del meta[key]
    meta.update(cls_meta)
    return meta


class ModelMetaclass(type):

    """
    Metaclass for Model objects

    Merges model metadata, sets the ``meta_`` attribute, and performs
    validation checks.

    """
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
    abstract : bool
        If a model is abstract then it has no table in Dynamo
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
        self._abstract = False
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

    def pk_dict(self, obj=None, scope=None, ddb_dump=False):
        """ Get the dynamo primary key dict for an item """
        # If we can unambiguously tell that a single string defines the primary
        # key, allow scope to be a single string
        if (obj is None and isinstance(scope, basestring) and
                self.range_key is None):
            scope = {self.hash_key.name: scope}

        hk = self.hk(obj, scope)
        if ddb_dump:
            hk = self.hash_key.ddb_dump(hk)
        rk = self.rk(obj, scope)
        key_dict = {self.hash_key.name: hk}
        if rk is not None:
            if ddb_dump:
                rk = self.range_key.ddb_dump(rk)
            key_dict[self.range_key.name] = rk
        return key_dict

    @property
    def abstract(self):
        """ Getter for abstract """
        return self._abstract

    @property
    def ddb_tablename(self):
        """ The name of the DynamoDB table """
        if self.abstract:
            return None
        return '-'.join(self.namespace + [self.name])

    def ddb_table(self, connection):
        """ Construct a Dynamo table from a connection """
        if self.abstract:
            return None
        return Table(self.ddb_tablename, connection=connection)

    def validate_model(self):
        """ Perform validation checks on the model declaration """
        if self.abstract or self.model.__dict__.get('__abstract__'):
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
        if self.abstract:
            return None
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
        if self.abstract:
            return None
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
