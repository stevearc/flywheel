""" Query and Scan builders """
import six

from .fields import Condition


class EntityNotFoundException(ValueError):

    """ Raised when results are expected and not found. """


class DuplicateEntityException(ValueError):

    """ Raised when too many results are found. """


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
    def dynamo(self):
        """ Shortcut to access DynamoDBConnection """
        return self.engine.dynamo

    @property
    def tablename(self):
        """ Shortcut to access dynamo table name """
        return self.model.meta_.ddb_tablename(self.engine.namespace)

    def gen(self, desc=False, consistent=False, attributes=None,
            filter_or=False, exclusive_start_key=None):
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
            iterate over dicts instead of model objects.
        filter_or : bool, optional
            If True, multiple filter() constraints will be joined with an OR
            (default AND).
        exclusive_start_key : dict, optional
            The ExclusiveStartKey to resume a previous query

        Returns
        -------
        results : generator

        """
        kwargs = self.condition.query_kwargs(self.model)
        if attributes is not None:
            kwargs['attributes'] = attributes
        results = self.dynamo.query(
            self.tablename, desc=desc, consistent=consistent,
            filter_or=filter_or, exclusive_start_key=exclusive_start_key,
            **kwargs)
        for result in results:
            if attributes is not None:
                yield result
            else:
                yield self.model.ddb_load_(self.engine, result)

    def __iter__(self):
        return self.gen()

    def all(self, desc=False, consistent=False, attributes=None,
            filter_or=False, exclusive_start_key=None):
        """
        Return the query results as a list

        Parameters
        ----------
        desc : bool, optional
            Return results in descending order (default False)
        consistent : bool, optional
            Force a consistent read of the data (default False)
        attributes : list, optional
            List of fields to retrieve from dynamo. If supplied, returns dicts
            instead of model objects.
        filter_or : bool, optional
            If True, multiple filter() constraints will be joined with an OR
            (default AND).
        exclusive_start_key : dict, optional
            The ExclusiveStartKey to resume a previous query

        Returns
        -------
        results : list

        """
        return list(self.gen(desc=desc, consistent=consistent,
                             attributes=attributes, filter_or=filter_or,
                             exclusive_start_key=exclusive_start_key))

    def count(self, filter_or=False):
        """
        Find the number of elements the match this query

        Parameters
        ----------
        filter_or : bool, optional
            If True, multiple filter() constraints will be joined with an OR
            (default AND).

        Returns
        -------
        count : int

        """
        kwargs = self.condition.query_kwargs(self.model)
        return self.dynamo.query(self.tablename, count=True,
                                 filter_or=filter_or, **kwargs)

    def first(self, desc=False, consistent=False, attributes=None,
              filter_or=False):
        """
        Return the first result of the query, or None if no results

        Parameters
        ----------
        desc : bool, optional
            Return results in descending order (default False)
        consistent : bool, optional
            Force a consistent read of the data (default False)
        attributes : list, optional
            List of fields to retrieve from dynamo. If supplied, returns dicts
            instead of model objects.
        filter_or : bool, optional
            If True, multiple filter() constraints will be joined with an OR
            (default AND).

        Returns
        -------
        result : :class:`~flywheel.models.Model` or None

        """
        self.limit(1)
        for result in self.gen(desc=desc, consistent=consistent,
                               attributes=attributes, filter_or=filter_or):
            return result
        return None

    def one(self, consistent=False, attributes=None, filter_or=False):
        """
        Return the result of the query. If there is not exactly one result,
        raise a ValueError

        Parameters
        ----------
        consistent : bool, optional
            Force a consistent read of the data (default False)
        attributes : list, optional
            List of fields to retrieve from dynamo. If supplied, returns dicts
            instead of model objects.
        filter_or : bool, optional
            If True, multiple filter() constraints will be joined with an OR
            (default AND).

        Returns
        -------
        result : :class:`~flywheel.models.Model`

        Raises
        ------
        e1 : :class:`~.EntityNotFoundException`
            If no entity is found. Subclasses :class:`~ValueError`.
        e2 : :class:`~.DuplicateEntityException`
            If more than one entity is found. Subclasses :class:`~ValueError`.

        """
        self.limit(2)
        items = self.all(consistent=consistent, attributes=attributes,
                         filter_or=filter_or)
        if len(items) > 1:
            raise DuplicateEntityException("More than one result!")
        elif len(items) == 0:
            raise EntityNotFoundException("Expected one result!")
        return items[0]

    def limit(self, count):
        """ Limit the number of query results """
        self.condition &= Condition.construct_limit(count)
        return self

    def index(self, name):
        """ Use a specific local or global index for the query """
        self.condition &= Condition.construct_index(name)
        return self

    def delete(self, filter_or=False):
        """
        Delete all items that match the query

        Parameters
        ----------
        filter_or : bool, optional
            If True, multiple filter() constraints will be joined with an OR
            (default AND).

        """
        meta = self.model.meta_
        attrs = [meta.hash_key.name]
        if meta.range_key is not None:
            attrs.append(meta.range_key.name)
        results = self.gen(attributes=attrs, filter_or=filter_or)
        return self.engine.delete_key(self.model, results)

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
        for key, val in six.iteritems(kwargs):
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

    def gen(self, attributes=None, desc=False, consistent=False,
            filter_or=False, exclusive_start_key=None):
        if desc:
            raise ValueError("Cannot order scan() results")
        if consistent:
            raise ValueError("Cannot force consistent read on scan()")
        kwargs = self.condition.scan_kwargs()

        if attributes is not None:
            kwargs['attributes'] = attributes
        results = self.dynamo.scan(
            self.tablename, filter_or=filter_or,
            exclusive_start_key=exclusive_start_key, **kwargs)
        for result in results:
            if attributes is not None:
                yield result
            else:
                yield self.model.ddb_load_(self.engine, result)

    def count(self, filter_or=False):
        kwargs = self.condition.scan_kwargs()
        return self.dynamo.scan(self.tablename, count=True, filter_or=False,
                                **kwargs)

    def index(self, name):
        raise TypeError("Scan cannot use an index!")
