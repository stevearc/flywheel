""" Query engine """
import itertools

from boto.dynamodb2 import connect_to_region
from boto.dynamodb2.exceptions import ConditionalCheckFailedException
from boto.dynamodb2.items import Item
from boto.dynamodb2.table import Table
from boto.dynamodb2.types import Dynamizer
from collections import defaultdict

from .fields import Condition
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

    def gen(self, reverse=False, consistent=False, attributes=None):
        """
        Return the query results as a generator

        Parameters
        ----------
        reverse : bool, optional
            Return results in reverse order (default False)
        consistent : bool, optional
            Force a consistent read of the data (default False)
        attributes : list, optional
            List of fields to retrieve from dynamo. If supplied, gen() will
            iterate over boto ResultItems instead of model objects.

        """
        kwargs = self.condition.query_kwargs(self.model)
        if attributes is not None:
            kwargs['attributes'] = attributes
        results = self.table.query(**kwargs)
        for result in results:
            if attributes is not None:
                yield result
            else:
                yield self.model.ddb_load(self.engine, result)

    def all(self, reverse=False, consistent=False, attributes=None):
        """
        Return the query results as a list

        Parameters
        ----------
        reverse : bool, optional
            Return results in reverse order (default False)
        consistent : bool, optional
            Force a consistent read of the data (default False)
        attributes : list, optional
            List of fields to retrieve from dynamo. If supplied, returns boto
            ResultItems instead of model objects.

        """
        return list(self.gen(reverse=reverse, consistent=consistent,
                             attributes=attributes))

    def first(self, reverse=False, consistent=False, attributes=None):
        """
        Return the first result of the query, or None if no results

        Parameters
        ----------
        reverse : bool, optional
            Return results in reverse order (default False)
        consistent : bool, optional
            Force a consistent read of the data (default False)
        attributes : list, optional
            List of fields to retrieve from dynamo. If supplied, returns boto
            ResultItems instead of model objects.

        """
        for result in self.gen(reverse=reverse, consistent=consistent,
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

        """
        result = None
        for item in self.gen(consistent=consistent, attributes=attributes):
            if result is not None:
                raise ValueError("More than one result!")
            else:
                result = item
        if result is None:
            raise ValueError("Expected one result!")
        return result

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
        count = 0
        meta = self.model.meta_
        with self.table.batch_write() as batch:
            attrs = [meta.hash_key.name]
            if meta.range_key is not None:
                attrs.append(meta.range_key.name)
            for result in self.gen(attributes=attrs):
                # Pull out just the hash and range key from the item
                kwargs = {meta.hash_key.name: result[meta.hash_key.name]}
                if meta.range_key is not None:
                    kwargs[meta.range_key.name] = result[meta.range_key.name]
                count += 1
                batch.delete_item(**kwargs)
        return count

    def filter(self, *conditions, **kwargs):
        """
        Add a Condition to constrain the query

        Notes
        -----
        The conditions may be passed in as positional arguments::

            engine.query(User).filter(User.id == 12345)

        Or they may be passed in as keyword arguments::

            engine.query(User).filter(firstname='Monty', lastname='Python')

        The limitations of the keyword method is that you may only create
        equality conditions.

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

    def gen(self, attributes=None, reverse=False, consistent=False):
        if reverse:
            raise ValueError("Cannot reverse scan() results")
        if consistent:
            raise ValueError("Cannot force consistent read on scan()")
        kwargs = self.condition.scan_kwargs()
        # TODO: uncomment this line to make scan deletes more efficient once
        # boto API supports passing 'attributes' to Table.scan()
        # if attributes is not None:
        #    kwargs['attributes'] = attributes
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
    basic example of saving items::

        item1 = MyModel()
        engine.save(item1)
        item1.foobar = 'baz'
        item2 = MyModel()
        engine.save([item1, item2], overwrite=True)

    You can also use the engine to query tables::

        user = engine.query(User).filter(User.id == 'abcdef).first()

        # calling engine() is a shortcut for engine.query()
        user = engine(User).filter(User.id == 'abcdef).first()

        d_users = engine(User).filter(User.school == 'MIT')\
                .filter(User.name.beginswith_('D')).all()

        # You can use logical and to join filter conditions
        d_users = engine(User).filter((User.school == 'MIT') &
                                            (User.name.beginswith_('D'))).all()

        # You can pass in equality constraints as keyword args
        user = engine(User).filter(id='abcdef').first()

    Scans are like queries, except that they don't use an index. Scans iterate
    over the ENTIRE TABLE so they are REALLY SLOW. Scans have access to
    additional filter conditions such as "contains" and "in"::

        # This is suuuuuper slow!
        user = engine.scan(User).filter(id='abcdef').first()

        # If you're doing an extremely large scan, you should tell it to return
        # a generator
        all_users = engine.scan(User).gen()

        # to filter a field not specified in the model declaration:
        prince = engine.scan(User).filter(User.field_('bio').beginswith_(
                   'Now this is a story all about how')).first()

    The default_atomic setting configures the behavior of save(), sync(), and
    delete(). Below is an explanation of the different values of
    default_atomic.

    **'update'**
    * save - overwrite defaults to True
    * sync - atomic defaults to True
    * delete - atomic defaults to False

    **True**
    * save - overwrite defaults to False
    * sync - atomic defaults to True
    * delete - atomic defaults to True

    **False**
    * save - overwrite defaults to True
    * sync - atomic defaults to False
    * delete - atomic defaults to False

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
        """ Get the default_atomic value """
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
        """ Create a table query for a specific model """
        return Query(self, model)

    def scan(self, model):
        """ Create a table scan for a specific model """
        return Scan(self, model)

    def delete(self, items, atomic=None):
        """
        Delete items from dynamo

        Parameters
        ----------
        items : list or :class:`~flywheel.model.Model`
            List of :class:`~flywheel.models.Model` objects to delete
        atomic : bool, optional
            If True, raise exception if the object has changed out from under
            us (default set by default_atomic)

        Raises
        ------
        exc : :class:`~boto.dynamodb2.exceptions.ConditionalCheckFailedException`
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
                    self.dynamo.delete_item(tablename, item.pk_dict_,
                                            expected=expected)
            else:
                table = Table(tablename, connection=self.dynamo)
                with table.batch_write() as batch:
                    for item in items:
                        batch.delete_item(**item.pk_dict_)

    def save(self, items, overwrite=None):
        """
        Save models to dynamo

        Parameters
        ----------
        items : list or :class:`~flywheel.models.Model`
        overwrite : bool, optional
            If False, raise exception if item already exists (default set by
            default_atomic)

        Raises
        ------
        exc : :class:`~boto.dynamodb2.exceptions.ConditionalCheckFailedException`
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
            If True, force a consistent read from the db. This will only take
            effect if the sync is only performing a read. (default False)

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
            us (default set by default_atomic)
        consistent : bool, optional
            If True, force a consistent read from the db. This will only take
            effect if the sync is only performing a read. (default False)

        Raises
        ------
        exc : :class:`~boto.dynamodb2.exceptions.ConditionalCheckFailedException`
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
                    if expect['Exists']:
                        expect['Value'] = DYNAMIZER.encode(cache_val)
                    expected[name] = expect

            # Atomic increment fields
            for name, value in item.__incrs__.iteritems():
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

            key = {item.meta_.hash_key.name: DYNAMIZER.encode(item.hk_)}
            if item.meta_.range_key is not None:
                key[item.meta_.range_key.name] = DYNAMIZER.encode(item.rk_)

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
