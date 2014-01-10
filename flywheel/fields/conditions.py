""" Query constraints """


class Condition(object):

    """
    A constraint that will be applied to a query or scan

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
        """ Get the kwargs for doing a table scan """
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
