""" Query engine """
import itertools

from boto.dynamodb2 import connect_to_region
from boto.dynamodb2.exceptions import ConditionalCheckFailedException
from boto.dynamodb2.items import Item
from boto.dynamodb2.table import Table
from boto.dynamodb2.types import Dynamizer
from collections import defaultdict

from .fields import Field
from .models import Model, ModelMetadata, SetDelta
from .query import Query, Scan


DYNAMIZER = Dynamizer()


class Engine(object):

    """
    Query engine for models

    Parameters
    ----------
    dynamo : :class:`boto.dynamodb2.DynamoDBConnection`, optional
    namespace : list, optional
        List of namespace component strings for models
    default_atomic : {True, False, 'update'}, optional
        Default setting for delete(), save(), and sync() (default 'update')

    Notes
    -----
    The engine is used to save, sync, delete, and query DynamoDB. Here is a
    basic example of saving items:

    .. code-block:: python

        item1 = MyModel()
        engine.save(item1)
        item1.foobar = 'baz'
        item2 = MyModel()
        engine.save([item1, item2], overwrite=True)

    You can also use the engine to query tables:

    .. code-block:: python

        user = engine.query(User).filter(User.id == 'abcdef).first()

        # calling engine() is a shortcut for engine.query()
        user = engine(User).filter(User.id == 'abcdef).first()

        d_users = engine(User).filter(User.school == 'MIT',
                                      User.name.beginswith_('D')).all()

        # You can pass in equality constraints as keyword args
        user = engine(User).filter(id='abcdef').first()

    Scans are like queries, except that they don't use an index. Scans iterate
    over the ENTIRE TABLE so they are REALLY SLOW. Scans have access to
    additional filter conditions such as "contains" and "in".

    .. code-block:: python

        # This is suuuuuper slow!
        user = engine.scan(User).filter(id='abcdef').first()

        # If you're doing an extremely large scan, you should tell it to return
        # a generator
        all_users = engine.scan(User).gen()

        # to filter a field not specified in the model declaration:
        prince = engine.scan(User).filter(User.field_('bio').beginswith_(
                   'Now this is a story all about how')).first()

    """

    def __init__(self, dynamo=None, namespace=None, default_atomic='update'):
        self.dynamo = dynamo
        self.models = {}
        self.namespace = namespace or []
        ModelMetadata.namespace = self.namespace
        self._default_atomic = None
        self.default_atomic = default_atomic

    @property
    def default_atomic(self):
        """
        Get the default_atomic value

        Notes
        -----
        The default_atomic setting configures the behavior of save(), sync(), and
        delete(). Below is an explanation of the different values of
        default_atomic.

        +----------------+--------+-----------------+
        | default_atomic | method | default         |
        +================+========+=================+
        |**'update'**    |        |                 |
        +----------------+--------+-----------------+
        |                | save   | overwrite=True  |
        +----------------+--------+-----------------+
        |                | sync   | atomic=True     |
        +----------------+--------+-----------------+
        |                | delete | atomic=False    |
        +----------------+--------+-----------------+
        |**True**        |        |                 |
        +----------------+--------+-----------------+
        |                | save   | overwrite=False |
        +----------------+--------+-----------------+
        |                | sync   | atomic=True     |
        +----------------+--------+-----------------+
        |                | delete | atomic=True     |
        +----------------+--------+-----------------+
        |**False**       |        |                 |
        +----------------+--------+-----------------+
        |                | save   | overwrite=True  |
        +----------------+--------+-----------------+
        |                | sync   | atomic=False    |
        +----------------+--------+-----------------+
        |                | delete | atomic=False    |
        +----------------+--------+-----------------+

        """
        return self._default_atomic

    @default_atomic.setter
    def default_atomic(self, default_atomic):
        """ Protected setter for default_atomic """
        if default_atomic not in (True, False, 'update'):
            raise ValueError("Unrecognized value '%s' for default_atomic" %
                             default_atomic)
        self._default_atomic = default_atomic

    def connect_to_region(self, region, **kwargs):
        """ Connect to an AWS region """
        self.dynamo = connect_to_region(region, **kwargs)

    def register(self, *models):
        """
        Register one or more models with the engine

        Registering is required for schema creation or deletion

        """
        for model in models:
            if model.meta_.name in self.models:
                raise ValueError("Model name '%s' already registered!" %
                                 model.meta_.name)
            self.models[model.meta_.name] = model

    def create_schema(self, test=False):
        """ Create the DynamoDB tables required by the registered models """
        tablenames = self.dynamo.list_tables()['TableNames']
        changed = []
        for model in self.models.itervalues():
            result = model.meta_.create_dynamo_schema(self.dynamo, tablenames,
                                                      test=test, wait=True)
            if result:
                changed.append(result)
        return changed

    def delete_schema(self, test=False):
        """ Drop the DynamoDB tables for all registered models """
        tablenames = set(self.dynamo.list_tables()['TableNames'])
        changed = []
        for model in self.models.itervalues():
            result = model.meta_.delete_dynamo_schema(self.dynamo, tablenames,
                                                      test=test, wait=True)
            changed.append(result)
        return changed

    def get_schema(self):
        """ Get the schema for the registered models """
        schema = []
        for model in self.models.itervalues():
            schema.append(model.meta_.ddb_tablename)
        return schema

    def __call__(self, model):
        """ Shortcut for query """
        return self.query(model)

    def query(self, model):
        """
        Create a table query for a specific model

        Returns
        -------
        query : :class:`.Query`

        """
        return Query(self, model)

    def scan(self, model):
        """
        Create a table scan for a specific model

        Returns
        -------
        scan : :class:`.Scan`

        """
        return Scan(self, model)

    def get(self, model, pkeys=None, consistent=False, **kwargs):
        """
        Fetch one or more items from dynamo from the primary keys

        Parameters
        ----------
        model : :class:`~flywheel.models.Model`
        pkeys : list, optional
            List of primary key dicts
        consistent : bool, optional
            Perform a consistent read from dynamo (default False)
        **kwargs : dict
            If pkeys is None, fetch only a single item and use kwargs as the
            primary key dict.

        Returns
        -------
        items : list or object
            If pkeys is a list of key dicts, this will be a list of items. If
            pkeys is None and **kwargs is used, this will be a single object.

        Notes
        -----
        If the model being fetched has no range key, you may use strings
        instead of primary key dicts. ex:

        .. code-block:: python

            >>> class Item(Model):
            ...     id = Field(hash_key=True)
            ...
            >>> items = engine.get(Item, ['abc', 'def', '123', '456'])

        """
        if pkeys is not None:
            if len(pkeys) == 0:
                return []
            keys = [model.meta_.pk_dict(scope=key) for key in pkeys]
        else:
            keys = [model.meta_.pk_dict(scope=kwargs)]

        table = model.meta_.ddb_table(self.dynamo)
        raw_items = table.batch_get(keys=keys, consistent=consistent)
        items = [model.ddb_load_(self, raw_item) for raw_item in raw_items]
        if pkeys is not None:
            return items
        if len(items) > 0:
            return items[0]
        return None

    def delete_key(self, model, pkeys=None, **kwargs):
        """
        Delete one or more items from dynamo as specified by primary keys

        Parameters
        ----------
        model : :class:`~flywheel.models.Model`
        pkeys : list, optional
            List of primary key dicts
        **kwargs : dict
            If pkeys is None, delete only a single item and use kwargs as the
            primary key dict

        Returns
        -------
        count : int
            The number of deleted items

        Notes
        -----
        If the model being deleted has no range key, you may use strings
        instead of primary key dicts. ex:

        .. code-block:: python

            >>> class Item(Model):
            ...     id = Field(hash_key=True)
            ...
            >>> items = engine.delete_key(Item, ['abc', 'def', '123', '456'])

        """
        if pkeys is not None:
            keys = pkeys
        else:
            keys = [kwargs]

        count = 0
        table = Table(model.meta_.ddb_tablename, connection=self.dynamo)
        with table.batch_write() as batch:
            for key in keys:
                pkey = model.meta_.pk_dict(scope=key)
                batch.delete_item(**pkey)
                count += 1
        return count

    # Alias because it makes sense
    delete_keys = delete_key

    def delete(self, items, atomic=None):
        """
        Delete items from dynamo

        Parameters
        ----------
        items : list or :class:`~flywheel.model.Model`
            List of :class:`~flywheel.models.Model` objects to delete
        atomic : bool, optional
            If True, raise exception if the object has changed out from under
            us (default set by :attr:`.default_atomic`)

        Raises
        ------
        exc : :class:`boto.dynamodb2.exceptions.ConditionalCheckFailedException`
            If overwrite is False and an item already exists in the database

        Notes
        -----
        Due to the structure of the AWS API, deleting with atomic=False is much
        faster because the requests can be batched.

        """
        if atomic is None:
            atomic = self.default_atomic is True
        if isinstance(items, Model):
            items = [items]
        if not items:
            return
        tables = defaultdict(list)
        for item in items:
            tables[item.meta_.ddb_tablename].append(item)

        count = 0
        for tablename, items in tables.iteritems():
            if atomic:
                for item in items:
                    expected = item.construct_ddb_expects_()
                    count += 1
                    self.dynamo.delete_item(tablename, item.pk_dict_,
                                            expected=expected)
            else:
                table = Table(tablename, connection=self.dynamo)
                with table.batch_write() as batch:
                    for item in items:
                        if isinstance(item, Model):
                            keys = item.pk_dict_
                        else:
                            keys = dict(item)
                        count += 1
                        batch.delete_item(**keys)
        return count

    def save(self, items, overwrite=None):
        """
        Save models to dynamo

        Parameters
        ----------
        items : list or :class:`~flywheel.models.Model`
        overwrite : bool, optional
            If False, raise exception if item already exists (default set by
            :attr:`.default_atomic`)

        Raises
        ------
        exc : :class:`boto.dynamodb2.exceptions.ConditionalCheckFailedException`
            If overwrite is False and an item already exists in the database

        Notes
        -----
        Overwrite will replace the *entire* item with the new one, not just
        different fields. After calling save(overwrite=True) you are guaranteed
        that the item in the database is exactly the item you saved.

        Due to the structure of the AWS API, saving with overwrite=True is much
        faster because the requests can be batched.

        """
        if overwrite is None:
            overwrite = self.default_atomic is not True
        if isinstance(items, Model):
            items = [items]
        if not items:
            return
        tables = defaultdict(list)
        for item in items:
            tables[item.meta_.ddb_tablename].append(item)
        for tablename, items in tables.iteritems():
            table = Table(tablename, connection=self.dynamo)
            if overwrite:
                with table.batch_write() as batch:
                    for item in items:
                        item.pre_save_(self)
                        batch.put_item(data=item.ddb_dump_())
                        item.post_save_()
            else:
                for item in items:
                    expected = {}
                    for name in item.meta_.fields:
                        expected[name] = {
                            'Exists': False,
                        }
                    item.pre_save_(self)
                    boto_item = Item(table, data=item.ddb_dump_())
                    self.dynamo.put_item(tablename, boto_item.prepare_full(),
                                         expected=expected)
                    item.post_save_()

    def refresh(self, items, consistent=False):
        """
        Overwrite model data with freshest from database

        Parameters
        ----------
        items : list or :class:`~flywheel.models.Model`
            Models to sync
        consistent : bool, optional
            If True, force a consistent read from the db. (default False)

        """
        if isinstance(items, Model):
            items = [items]
        if not items:
            return

        tables = defaultdict(list)
        for item in items:
            tables[item.meta_.ddb_tablename].append(item)

        for tablename, items in tables.iteritems():
            table = Table(tablename, connection=self.dynamo)
            keys = [item.pk_dict_ for item in items]
            results = table.batch_get(keys, consistent=consistent)
            for item, data in itertools.izip(items, results):
                with item.loading_(self):
                    for key, val in data.items():
                        item.set_ddb_val_(key, val)

    def sync(self, items, atomic=None, consistent=False):
        """
        Sync model changes back to database

        This will push any updates to the database, and ensure that all of the
        synced items have the most up-to-date data.

        Parameters
        ----------
        items : list or :class:`~flywheel.models.Model`
            Models to sync
        atomic : bool, optional
            If True, raise exception if the object has changed out from under
            us (default set by :attr:`.default_atomic`)
        consistent : bool, optional
            If True, force a consistent read from the db. This will only take
            effect if the sync is only performing a read. (default False)

        Raises
        ------
        exc : :class:`boto.dynamodb2.exceptions.ConditionalCheckFailedException`
            If atomic=True and the model changed underneath us

        """
        if atomic is None:
            atomic = self.default_atomic is not False
        if isinstance(items, Model):
            items = [items]
        refresh_models = []
        for item in items:
            _atomic = atomic
            # Look for any mutable fields (e.g. sets) that have changed
            for name in item.keys_():
                field = item.meta_.fields.get(name)
                if field is None:
                    value = item.get_(name)
                    if Field.is_overflow_mutable(value):
                        if value != item.cached_(name):
                            item.__dirty__.add(name)
                elif field.is_mutable and name not in item.__dirty__:
                    cached_var = item.cached_(name)
                    if cached_var is None:
                        cached_var = field.default
                    if field.resolve(item) != cached_var:
                        item.__dirty__.add(name)

            if not item.__dirty__ and not item.__incrs__:
                refresh_models.append(item)
                continue
            fields = item.__dirty__
            item.pre_save_(self)

            # If the model has changed any field that is part of a composite
            # field, FORCE the sync to be atomic. This prevents the composite
            # key from potentially getting into an inconsistent state
            for name in itertools.chain(item.__incrs__, fields):
                for related_name in item.meta_.related_fields.get(name, []):
                    field = item.meta_.fields[related_name]
                    if field.composite:
                        _atomic = True
                        break

            if _atomic:
                expected = item.construct_ddb_expects_()
            else:
                expected = None

            # Set dynamo keys
            data = {}
            for name in fields:
                field = item.meta_.fields.get(name)
                value = getattr(item, name)
                # Empty sets can't be stored, so delete the value instead
                if value is None or value == set():
                    action = 'DELETE'
                else:
                    action = 'PUT'
                data[name] = {'Action': action}
                if action != 'DELETE':
                    data[name]['Value'] = DYNAMIZER.encode(
                        item.ddb_dump_field_(name))

            # Atomic increment fields
            for name, value in item.__incrs__.iteritems():
                # We don't need to ddb_dump because we know they're all native
                if isinstance(value, SetDelta):
                    data[name] = {
                        'Action': value.action,
                        'Value': DYNAMIZER.encode(value.values),
                    }
                else:
                    data[name] = {
                        'Action': 'ADD',
                        'Value': DYNAMIZER.encode(value),
                    }

            key = dict([(k, DYNAMIZER.encode(v)) for k, v in
                        item.pk_dict_.iteritems()])

            # Perform sync
            print "Setting values", data.keys()
            ret = self.dynamo.update_item(item.meta_.ddb_tablename, key, data,
                                          expected=expected,
                                          return_values='ALL_NEW')

            # Load updated data back into object
            table = item.meta_.ddb_table(self.dynamo)
            data = dict([(k, DYNAMIZER.decode(v)) for k, v in
                        ret.get('Attributes', {}).iteritems()])
            ret = Item(table, data=data)
            with item.loading_(self):
                for key, val in dict(ret).iteritems():
                    item.set_ddb_val_(key, val)

            item.post_save_()

        # Handle items that didn't have any fields to update

        # If the item isn't known to exist in the db, try to save it first
        for item in refresh_models:
            if not item.persisted_:
                try:
                    self.save(item, overwrite=False)
                except ConditionalCheckFailedException:
                    pass
        # Refresh item data
        self.refresh(refresh_models, consistent=consistent)
