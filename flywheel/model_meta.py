""" Model metadata and metaclass objects """
import six
import time

import inspect
from dynamo3 import DynamoKey, Throughput
from collections import defaultdict

from .fields import Field
from .fields.conditions import FILTER_ONLY


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

    def query_kwargs(self, eq_fields, fields):
        """ Get the query and filter kwargs for querying against this index """
        kwargs = {'%s__eq' % self.hash_key.name:
                  self.hash_key.resolve(scope=eq_fields)}
        if self.index_name is not None:
            kwargs['index'] = self.index_name
        remaining = set(eq_fields)
        remaining = remaining.union(fields)
        remaining -= self.hash_key.can_resolve(eq_fields)
        if self.range_key is not None:
            eq_range_fields = self.range_key.can_resolve(eq_fields)
            if eq_range_fields:
                remaining -= eq_range_fields
                key = '%s__eq' % self.range_key.name
                val = self.range_key.resolve(scope=eq_fields)
                kwargs[key] = val
            else:
                for field in self.range_key.can_resolve(fields):
                    (op, val) = fields[field]
                    if op in FILTER_ONLY:
                        continue
                    kwargs['%s__%s' % (field, op)] = val
                    remaining.remove(field)

        # Find the additional filter arguments
        filter_fields = {}
        for key in remaining:
            if key in eq_fields:
                filter_fields['%s__eq' % key] = eq_fields[key]
            else:
                op, val = fields[key]
                filter_fields['%s__%s' % (key, op)] = val
        kwargs['filter'] = filter_fields

        return kwargs

    def pk_dict(self, obj=None, scope=None, ddb_dump=False):
        """ Get the dynamo primary key dict for this ordering """
        # If we can unambiguously tell that a single string defines the primary
        # key, allow scope to be a single string
        if (obj is None and isinstance(scope, six.string_types) and
                self.range_key is None):
            scope = {self.hash_key.name: scope}

        hk = self.hash_key.resolve(obj, scope)
        if ddb_dump:
            hk = self.hash_key.ddb_dump(hk)
        key_dict = {self.hash_key.name: hk}
        if self.range_key is not None:
            rk = self.range_key.resolve(obj, scope)
            if ddb_dump:
                rk = self.range_key.ddb_dump(rk)
            key_dict[self.range_key.name] = rk
        return key_dict

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
    for key in list(meta.keys()):
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
        if hasattr(cls, '__on_create__'):
            cls.__on_create__()

        if hasattr(cls, '__metadata_class__'):
            cls.meta_ = cls.__metadata_class__(cls)
            cls.meta_.post_create()
            cls.meta_.validate_model()
            cls.meta_.post_validate()

        if hasattr(cls, '__after_create__'):
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

    def __init__(self, model):
        self.model = model
        self._name = model.__name__
        self.global_indexes = []
        self.orderings = []
        self.throughput = Throughput()
        self._abstract = False
        self.__dict__.update(model.__metadata__)
        # Allow throughput to be specified as read/write in a dict
        # pylint: disable=E1134
        if isinstance(self.throughput, dict):
            self.throughput = Throughput(**self.throughput)
        # pylint: enable=E1134
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
                if name.startswith('__') or name.endswith('_'):
                    raise ValidationError("Field '%s' cannot begin with '__' "
                                          "or end with '_'" % name)
                self.fields[name] = member
                member.name = name
                member.model = self.model
                if member.hash_key:
                    self.hash_key = member
                elif member.range_key:
                    self.range_key = member

        self.orderings.append(self.__order_class__(self, self.hash_key,
                                                   self.range_key))
        for field in six.itervalues(self.fields):
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

        for field in six.itervalues(self.fields):
            self.related_fields[field.name].add(field.name)
            if field.composite:
                update_related(field, field.name)

    def get_ordering_from_fields(self, eq_fields, fields):
        """
        Get a unique ordering from constraint fields.

        This does a best-effort guess of which index is being queried. It
        prioritizes indexes that have a constraint on the range key. It
        prioritizes the primary key over local and global indexes.

        Parameters
        ----------
        eq_fields : list
            List of field names that are constrained with '='.
        fields : list
            List of field names that are constrained with inequality operators
            ('>', '<', 'beginswith', etc)

        Returns
        -------
        ordering : :class:`.Ordering`

        Raises
        ------
        exc : :class:`TypeError`
            If more than one possible Ordering is found

        """
        index_satisfied_orderings = []
        other_orderings = []
        eq_field_set = set(eq_fields)
        for order in self.orderings:
            needed = order.hash_key.can_resolve(eq_fields)
            # hash key could not be satisfied
            if len(needed) == 0:
                continue

            remaining = eq_field_set - needed

            if order.range_key is not None:
                if order.range_key.can_resolve(remaining) or order.range_key.can_resolve(fields):
                    index_satisfied_orderings.append(order)
                    continue
            other_orderings.append(order)

        def get_best_ordering(orderings):
            """ Find the best choice in a list of orderings. """
            if len(orderings) == 1:
                return orderings[0]
            else:
                for order in orderings:
                    if order.index_name is None:
                        return order
                raise ValueError("More than one ordering found: %s" % orderings)

        if index_satisfied_orderings:
            return get_best_ordering(index_satisfied_orderings)
        elif other_orderings:
            return get_best_ordering(other_orderings)
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

    def pk_tuple(self, obj=None, scope=None, ddb_dump=False, ddb_load=False):
        """ Get a tuple that represents the primary key for an item """
        hk = self.hk(obj, scope)
        if ddb_dump:
            hk = self.hash_key.ddb_dump(hk)
        elif ddb_load:
            hk = self.hash_key.ddb_load(hk)
        if self.range_key is None:
            return (hk,)
        rk = self.rk(obj, scope)
        if ddb_dump:
            rk = self.range_key.ddb_dump(rk)
        elif ddb_load:
            rk = self.range_key.ddb_load(rk)
        return (hk, rk)

    def pk_dict(self, obj=None, scope=None, ddb_dump=False):
        """ Get the dynamo primary key dict for an item """
        return self.index_pk_dict(None, obj, scope, ddb_dump)

    def index_pk_dict(self, index_name, obj=None, scope=None, ddb_dump=False):
        """ Get the primary key dict for an index (includes the table key) """
        # Get the 'table' index, which is the hash & range key
        table_order = self.get_ordering_from_index(None)
        pk = table_order.pk_dict(obj, scope, ddb_dump)
        if index_name is not None:
            ordering = self.get_ordering_from_index(index_name)
            pk.update(ordering.pk_dict(obj, scope, ddb_dump))
        return pk

    @property
    def abstract(self):
        """ Getter for abstract """
        return self._abstract

    def ddb_tablename(self, namespace=()):
        """
        The name of the DynamoDB table

        Parameters
        ----------
        namespace : list or str, optional
            String prefix or list of component parts of a prefix for the table
            name.  The prefix will be this string or strings (joined by '-').

        """
        if self.abstract:
            return None
        elif isinstance(namespace, six.string_types):
            return namespace + self.name
        else:
            return '-'.join(tuple(namespace) + (self.name,))

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
                             wait=False, throughput=None, namespace=()):
        """
        Create all Dynamo tables for this model

        Parameters
        ----------
        connection : :class:`~dynamo3.DynamoDBConnection`
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
        namespace : str or tuple, optional
            The namespace of the table

        Returns
        -------
        table : str
            Table name that was created, or None if nothing created

        """
        if self.abstract:
            return None
        if tablenames is None:
            tablenames = set(connection.list_tables())
        tablename = self.ddb_tablename(namespace)
        if tablename in tablenames:
            return None
        elif test:
            return tablename

        indexes = []
        global_indexes = []
        hash_key = None

        if throughput is not None:
            table_throughput = Throughput(throughput['read'],
                                          throughput['write'])
        else:
            table_throughput = self.throughput

        hash_key = DynamoKey(self.hash_key.name,
                             data_type=self.hash_key.ddb_data_type)
        range_key = None
        if self.range_key is not None:
            range_key = DynamoKey(self.range_key.name,
                                  data_type=self.range_key.ddb_data_type)
        for field in six.itervalues(self.fields):
            if field.index:
                idx = field.get_ddb_index()
                indexes.append(idx)

        for gindex in self.global_indexes:
            index = gindex.get_ddb_index(self.fields)
            if throughput is not None and gindex.name in throughput:
                index.throughput = Throughput(**throughput[gindex.name])
            global_indexes.append(index)

        if not test:
            connection.create_table(tablename, hash_key, range_key,
                                    indexes, global_indexes, table_throughput)
            if wait:
                desc = connection.describe_table(tablename)
                while desc.status != 'ACTIVE':
                    time.sleep(1)
                    desc = connection.describe_table(tablename)

        return tablename

    def delete_dynamo_schema(self, connection, tablenames=None, test=False,
                             wait=False, namespace=()):
        """
        Drop all Dynamo tables for this model

        Parameters
        ----------
        connection : :class:`~dynamo3.DynamoDBConnection`
        tablenames : list, optional
            List of tables that already exist. Will call 'describe' if not
            provided.
        test : bool, optional
            If True, don't actually delete the table (default False)
        wait : bool, optional
            If True, block until table has been deleted (default False)
        namespace : str or tuple, optional
            The namespace of the table

        Returns
        -------
        table : str
            Table name that was deleted, or None if nothing deleted

        """
        if self.abstract:
            return None
        if tablenames is None:
            tablenames = set(connection.list_tables())

        tablename = self.ddb_tablename(namespace)
        if tablename in tablenames:
            if not test:
                connection.delete_table(tablename)
                if wait:
                    desc = connection.describe_table(tablename)
                    while desc is not None:
                        desc = connection.describe_table(tablename)
            return tablename
        return None
