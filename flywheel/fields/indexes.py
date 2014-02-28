""" Index definitions """
from dynamo3 import DynamoKey, Throughput
from dynamo3 import GlobalIndex as GIndex


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
        self._throughput = Throughput()
        self.ddb_index = GIndex.all
        self.kwargs = {}

    @classmethod
    def all(cls, name, hash_key, range_key=None):
        """ Project all attributes into the index """
        return cls(name, hash_key, range_key)

    @classmethod
    def keys(cls, name, hash_key, range_key=None):
        """ Project key attributes into the index """
        index = cls(name, hash_key, range_key)
        index.ddb_index = GIndex.keys
        return index

    @classmethod
    def include(cls, name, hash_key, range_key=None, includes=None):
        """ Select which attributes to project into the index """
        includes = includes or []
        index = cls(name, hash_key, range_key)
        index.ddb_index = GIndex.include
        index.kwargs['includes'] = includes
        return index

    def get_ddb_index(self, fields):
        """ Get the dynamo index class for this GlobalIndex """
        hash_key = DynamoKey(self.hash_key,
                             data_type=fields[self.hash_key].ddb_data_type)
        range_key = None
        if self.range_key is not None:
            range_key = DynamoKey(self.range_key,
                                  data_type=fields[self.range_key].ddb_data_type)
        index = self.ddb_index(self.name, hash_key, range_key,
                               throughput=self._throughput, **self.kwargs)
        return index

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
        self._throughput = Throughput(read, write)
        return self

    def __contains__(self, field):
        return field == self.hash_key or field == self.range_key

    def __iter__(self):
        yield self.hash_key
        if self.range_key is not None:
            yield self.range_key

    def __repr__(self):
        if self.range_key is None:
            return "GlobalIndex('%s', '%s')" % (self.name, self.hash_key)
        else:
            return "GlobalIndex('%s', '%s', '%s')" % (self.name, self.hash_key,
                                                      self.range_key)

    def __str__(self):
        return repr(self)
