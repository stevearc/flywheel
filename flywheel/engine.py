""" Query engine """
import itertools

from boto.dynamodb2 import connect_to_region
from boto.dynamodb2.exceptions import ConditionalCheckFailedException
from boto.dynamodb2.items import Item
from boto.dynamodb2.table import Table
from boto.dynamodb2.types import Dynamizer
from collections import defaultdict

from .fields import Field, Condition
from .models import Model, ModelMetadata


DYNAMIZER = Dynamizer()


class Query(object):

    """
    An object used to query dynamo tables

    See the :class:`.Engine` for query examples

    Parameters
    ----------
    engine : :class:`.Engine`
    model : class
        Subclass of :class:`~flywheel.models.Model`

    """

    def __init__(self, engine, model):
        self.engine = engine
        self.model = model
        self.condition = Condition()

    @property
    def table(self):
        """ Shortcut to access dynamo table """
        return Table(self.model.meta_.ddb_tablename,
                     connection=self.engine.dynamo)

    def gen(self, desc=False, consistent=False, attributes=None):
        """
        Return the query results as a generator

        Parameters
        ----------
        desc : bool, optional
            Return results in descending order (default False)
        consistent : bool, optional
            Force a consistent read of the data (default False)
        attributes : list, optional
            List of fields to retrieve from dynamo. If supplied, gen() will
            iterate over boto ResultItems instead of model objects.

        """
        kwargs = self.condition.query_kwargs(self.model)
        if attributes is not None:
            kwargs['attributes'] = attributes
        kwargs['reverse'] = not desc
        kwargs['consistent'] = consistent
        results = self.table.query(**kwargs)
        for result in results:
            if attributes is not None:
                yield result
            else:
                yield self.model.ddb_load(self.engine, result)

    def all(self, desc=False, consistent=False, attributes=None):
        """
        Return the query results as a list

        Parameters
        ----------
        desc : bool, optional
            Return results in descending order (default False)
        consistent : bool, optional
            Force a consistent read of the data (default False)
        attributes : list, optional
            List of fields to retrieve from dynamo. If supplied, returns boto
            ResultItems instead of model objects.

        """
        return list(self.gen(desc=desc, consistent=consistent,
                             attributes=attributes))

    def first(self, desc=False, consistent=False, attributes=None):
        """
        Return the first result of the query, or None if no results

        Parameters
        ----------
        desc : bool, optional
            Return results in descending order (default False)
        consistent : bool, optional
            Force a consistent read of the data (default False)
        attributes : list, optional
            List of fields to retrieve from dynamo. If supplied, returns boto
            ResultItems instead of model objects.

        """
        self.limit(1)
        for result in self.gen(desc=desc, consistent=consistent,
                               attributes=attributes):
            return result
        return None

    def one(self, consistent=False, attributes=None):
        """
        Return the result of the query. If there is not exactly one result,
        raise a ValueError

        Parameters
        ----------
        consistent : bool, optional
            Force a consistent read of the data (default False)
        attributes : list, optional
            List of fields to retrieve from dynamo. If supplied, returns boto
            ResultItems instead of model objects.

        Raises
        ------
        exc : ValueError
            If there is not exactly one result

        """
        self.limit(2)
        items = self.all(consistent=consistent, attributes=attributes)
        if len(items) > 1:
            raise ValueError("More than one result!")
        elif len(items) == 0:
            raise ValueError("Expected one result!")
        return items[0]

    def limit(self, count):
        """ Limit the number of query results """
        self.condition &= Condition.construct_limit(count)
        return self

    def index(self, name):
        """ Use a specific local or global index for the query """
        self.condition &= Condition.construct_index(name)
        return self

    def delete(self):
        """ Delete all items that match the query """
        meta = self.model.meta_
        attrs = [meta.hash_key.name]
        if meta.range_key is not None:
            attrs.append(meta.range_key.name)
        results = self.gen(attributes=attrs)
        # TODO: uncomment this line once boto API supports passing 'attributes'
        # to Table.scan()
        # return self.engine.delete_key(self.model, results)
        return self.engine._delete_items(self.model.meta_.ddb_tablename,
                                         results, atomic=False)

    def filter(self, *conditions, **kwargs):
        """
        Add a Condition to constrain the query

        Notes
        -----
        The conditions may be passed in as positional arguments:

        .. code-block:: python

            engine.query(User).filter(User.id == 12345)

        Or they may be passed in as keyword arguments:

        .. code-block:: python

            engine.query(User).filter(firstname='Monty', lastname='Python')

        The limitations of the keyword method is that you may only create
        equality conditions. You may use both types in a single filter:

        .. code-block:: python

            engine.query(User).filter(User.num_friends > 10, name='Monty')

        """
        for condition in conditions:
            self.condition &= condition
        for key, val in kwargs.iteritems():
            field = self.model.meta_.fields.get(key)
            if field is not None:
                self.condition &= (field == val)
            else:
                self.condition &= (self.model.field_(key) == val)

        return self


class Scan(Query):

    """
    An object used to scan dynamo tables

    scans are like Queries except they don't use indexes. This means they
    iterate over all data in the table and are SLOW

    Parameters
    ----------
    engine : :class:`.Engine`
    model : class
        Subclass of :class:`~flywheel.models.Model`

    """

    def gen(self, attributes=None, desc=False, consistent=False):
        if desc:
            raise ValueError("Cannot order scan() results")
        if consistent:
            raise ValueError("Cannot force consistent read on scan()")
        kwargs = self.condition.scan_kwargs()

        # TODO: delete this line to make scan deletes more efficient once
        # boto API supports passing 'attributes' to Table.scan()
        attributes = None

        if attributes is not None:
            kwargs['attributes'] = attributes
        results = self.table.scan(**kwargs)
        for result in results:
            if attributes is not None:
                yield result
            else:
                yield self.model.ddb_load(self.engine, result)

    def index(self, name):
        raise TypeError("Scan cannot use an index!")


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
        items = [model.ddb_load(self, raw_item) for raw_item in raw_items]
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

        for tablename, items in tables.iteritems():
            self._delete_items(tablename, items, atomic)

    def _delete_items(self, tablename, items, atomic):
        """ Delete items from a single table """
        count = 0
        if atomic:
            for item in items:
                expected = {}
                for name in item.keys_():
                    val = getattr(item, name)
                    exists = val is not None
                    expected[name] = {
                        'Exists': exists,
                    }
                    if exists:
                        expected[name]['Value'] = DYNAMIZER.encode(val)
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
                        item.pre_save(self)
                        batch.put_item(data=item.ddb_dump())
                        item.post_save()
            else:
                for item in items:
                    expected = {}
                    for name in item.meta_.fields:
                        expected[name] = {
                            'Exists': False,
                        }
                    item.pre_save(self)
                    boto_item = Item(table, data=item.ddb_dump())
                    self.dynamo.put_item(tablename, boto_item.prepare_full(),
                                         expected=expected)
                    item.post_save()

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
                with item.loading(self):
                    for key, val in data.items():
                        item.set_ddb_val(key, val)

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
            for name, field in item.meta_.fields.iteritems():
                if field.is_mutable and name not in item.__dirty__:
                    cached_var = item.cached_(name)
                    if cached_var is None:
                        cached_var = field.default
                    if field.resolve(item) != cached_var:
                        item.__dirty__.add(name)

            if not item.__dirty__ and not item.__incrs__:
                refresh_models.append(item)
                continue
            fields = item.__dirty__
            item.pre_save(self)

            # If the model has incremented any field that is part of a
            # composite field, FORCE the sync to be atomic. This prevents the
            # composite key from potentially getting into an inconsistent state
            # in the database
            for name in item.__incrs__:
                for related_name in item.meta_.related_fields[name]:
                    field = item.meta_.fields[related_name]
                    if field.composite:
                        _atomic = True
                        break

            if _atomic:
                expected = {}
            else:
                expected = None

            # Set dynamo keys
            data = {}
            for name in fields:
                value = getattr(item, name)
                if value is None:
                    action = 'DELETE'
                else:
                    action = 'PUT'
                data[name] = {'Action': action}
                if action != 'DELETE':
                    data[name]['Value'] = DYNAMIZER.encode(
                        item.ddb_dump_field(name))
                if _atomic:
                    cache_val = item.cached_(name)
                    expect = {
                        'Exists': cache_val is not None,
                    }
                    field = item.meta_.fields.get(name)
                    if field is not None:
                        cache_val = field.ddb_dump(cache_val)
                    else:
                        cache_val = Field.ddb_dump_overflow(cache_val)
                    if expect['Exists']:
                        expect['Value'] = DYNAMIZER.encode(cache_val)
                    expected[name] = expect

            # Atomic increment fields
            for name, value in item.__incrs__.iteritems():
                # We don't need to ddb_dump because we know they're all numbers
                data[name] = {'Action': 'ADD'}
                data[name]['Value'] = DYNAMIZER.encode(value)
                if _atomic:
                    cache_val = item.cached_(name)
                    expect = {
                        'Exists': cache_val is not None,
                    }
                    if expect['Exists']:
                        expect['Value'] = DYNAMIZER.encode(cache_val)
                    expected[name] = expect

            key = dict([(k, DYNAMIZER.encode(v)) for k, v in
                        item.pk_dict_.iteritems()])

            # Perform sync
            ret = self.dynamo.update_item(item.meta_.ddb_tablename, key, data,
                                          expected=expected,
                                          return_values='ALL_NEW')

            # Load updated data back into object
            table = item.meta_.ddb_table(self.dynamo)
            data = dict([(k, DYNAMIZER.decode(v)) for k, v in
                        ret.get('Attributes', {}).iteritems()])
            ret = Item(table, data=data)
            with item.loading(self):
                for key, val in dict(ret).iteritems():
                    item.set_ddb_val(key, val)

            item.post_save()

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
