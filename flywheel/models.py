""" Model code """
import six
import contextlib
import copy
import itertools

from dynamo3 import is_null
from .fields import Field, NUMBER
from .model_meta import ModelMetaclass, ModelMetadata, Ordering

# pylint: disable=E1002

SENTINEL = object()


class SetDelta(object):

    """
    Wrapper for an atomic change to a Dynamo set

    Used to track the changes when using :meth:`~.Model.add_` and
    :meth:`~.Model.remove_`

    """

    def __init__(self):
        self.action = None
        self.values = set()

    def merge(self, other):
        """
        Merge the delta with a set

        Parameters
        ----------
        other : set
            The original set to merge the changes with

        """
        if other is None:
            other = set()
        new = set()
        new.update(other)
        if self.action == 'ADD':
            new.update(self.values)
        elif other.issuperset(self.values):
            new.difference_update(self.values)
        else:
            raise KeyError("Cannot remove values that are not in the set!")

        return new

    def add(self, action, value):
        """
        Add another update to the delta

        Parameters
        ----------
        action : {'ADD', 'DELETE'}
        value : object
            The value to add or remove

        """
        if action not in ('ADD', 'DELETE'):
            raise ValueError("Invalid action '%s'" % action)
        if self.action is None:
            self.action = action

        if action == self.action:
            if isinstance(value, set):
                self.values.update(value)
            else:
                self.values.add(value)
        else:
            if not isinstance(value, set):
                value = set([value])
            if self.values.issuperset(value):
                self.values.difference_update(value)
            else:
                raise ValueError("Cannot ADD and REMOVE items from the same "
                                 "set in the same update")


class Model(six.with_metaclass(ModelMetaclass)):

    """
    Base class for all tube models

    For documentation on the metadata fields, check the attributes on the
    :class:`.ModelMetadata` class.

    Attributes
    ----------
    __metadata_class__ : class
        The class that is instantiated and set as ``meta_``
    __metadata__ : dict
        For details see :ref:`metadata`
    meta_ : :class:`~.ModelMetadata`
        The metadata for the model
    __engine__ : :class:`~flywheel.engine.Engine`
        Cached copy of the Engine that was used to save/load the model. This
        will be set after saving or loading a model.
    __dirty__ : set
        The set of all immutable fields that have been changed since the last
        save operation.
    __cache__ : dict
        The last seen value that was stored in the database. This is used to
        construct the ``expects`` dict when making updates that raise on
        conflict.
    __incrs__ : dict
        Mapping of fields to atomic add/delete operations for numbers and sets.

    """
    __metadata_class__ = ModelMetadata
    __metadata__ = {
        '_abstract': True,
    }
    meta_ = None
    __engine__ = None
    __dirty__ = None
    __cache__ = None
    __incrs__ = None
    # dict of all undeclared fields that have been set
    _overflow = None
    _persisted = False
    _loading = False

    def __init__(self, *args, **kwargs):  # pylint: disable=W0231
        if len(args) > 2 or (len(args) > 1 and self.meta_.range_key is None):
            raise TypeError("Too many positional arguments!")
        if len(args) > 0:
            setattr(self, self.meta_.hash_key.name, args[0])
        if len(args) > 1:
            setattr(self, self.meta_.range_key.name, args[1])
        for key, value in six.iteritems(kwargs):
            setattr(self, key, value)

    def refresh(self, consistent=False):
        """ Overwrite model data with freshest from database """
        if self.__engine__ is None:
            raise ValueError("Cannot refresh: No DB connection")

        self.__engine__.refresh(self, consistent=consistent)

    def save(self, overwrite=None):
        """ Save model data to database (see also: sync) """
        if self.__engine__ is None:
            raise ValueError("Cannot save: No DB connection")

        self.__engine__.save(self, overwrite=overwrite)

    def sync(self, raise_on_conflict=None, constraints=None):
        """ Sync model changes back to database """
        if self.__engine__ is None:
            raise ValueError("Cannot sync: No DB connection")

        self.__engine__.sync(self, raise_on_conflict=raise_on_conflict,
                             constraints=constraints)

    def delete(self, raise_on_conflict=None):
        """ Delete the model from the database """
        if self.__engine__ is None:
            raise ValueError("Cannot delete: No DB connection")
        self.__engine__.delete(self, raise_on_conflict=raise_on_conflict)

    @classmethod
    def __on_create__(cls):
        """ Called after class is constructed but before meta_ is set """
        pass

    @classmethod
    def __after_create__(cls):
        """ Called after class is constructed and after meta_ is set """
        pass

    def __new__(cls, *_, **__):
        """ Override __new__ to set default field values """
        obj = super(Model, cls).__new__(cls)
        mark_dirty = []
        with obj.loading_():
            for name, field in six.iteritems(cls.meta_.fields):
                if not field.composite:
                    setattr(obj, name, field.default)
                    if not is_null(field.default):
                        mark_dirty.append(name)
        obj.__dirty__.update(mark_dirty)
        obj._overflow = {}
        obj._persisted = False
        return obj

    def _is_field_primary(self, key):
        """ Check if a given field is part of the primary key """
        return ((self.meta_.hash_key.name in self.meta_.related_fields[key]) or
                (self.meta_.range_key is not None and
                 self.meta_.range_key.name in self.meta_.related_fields[key]))

    def __setattr__(self, name, value):
        if self.persisted_:
            if self._is_field_primary(name):
                if value != getattr(self, name):
                    raise AttributeError(
                        "Cannot change an item's primary key!")
                else:
                    return
        field = self.meta_.fields.get(name)
        if field is not None:
            # Ignore if trying to set a composite field
            if field.composite:
                return
            # Mutable fields check if they're dirty during sync()
            if field.is_mutable:
                return super(Model, self).__setattr__(name, field.coerce(value))

            # Don't mark the field dirty if the new and old values are the same
            oldv = getattr(self, name, SENTINEL)
            try:
                same_value = oldv is not SENTINEL and oldv == value
            except Exception:
                same_value = False
            if not self._loading and same_value:
                return
            self.mark_dirty_(name)
            if (not self._loading and self.persisted_ and
                    name not in self.__cache__):
                for related in self.meta_.related_fields[name]:
                    cached_var = copy.copy(getattr(self, related))
                    self.__cache__[related] = cached_var
            return super(Model, self).__setattr__(name, field.coerce(value))
        elif name.startswith('_') or name.endswith('_'):
            # Don't interfere with non-Field private attrs
            return super(Model, self).__setattr__(name, value)
        else:
            self.mark_dirty_(name)
            if (not self._loading and self.persisted_ and name not in
                    self.__cache__):
                if Field.is_overflow_mutable(value):
                    self.__cache__[name] = copy.copy(self.get_(name))
                else:
                    self.__cache__[name] = self.get_(name)
            self._overflow[name] = value

    def __delattr__(self, name):
        field = self.meta_.fields.get(name)
        if field is not None:
            if not field.composite:
                setattr(self, name, None)
        elif name.startswith('_') or name.endswith('_'):
            # Don't interfere with non-Field private attrs
            super(Model, self).__delattr__(name)
            return
        else:
            setattr(self, name, None)

    def __getattribute__(self, name):
        if not name.startswith('__') and not name.endswith('_'):
            # Don't interfere with magic attrs or attrs ending in '_'
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

    def get_(self, name, default=None):
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
        """ The primary key dict, encoded for dynamo """
        return self.meta_.pk_dict(self, ddb_dump=True)

    def index_pk_dict_(self, index_name):
        """ The primary key dict for an index, encoded for dynamo """
        return self.meta_.index_pk_dict(index_name, self, ddb_dump=True)

    @property
    def pk_tuple_(self):
        """ The primary key dict, encoded for dynamo """
        return self.meta_.pk_tuple(self, ddb_dump=True)

    @property
    def persisted_(self):
        """ True if the model exists in DynamoDB, False otherwise """
        return self._persisted

    def keys_(self):
        """ All declared fields and any additional fields """
        return itertools.chain(self.meta_.fields.keys(), self._overflow.keys())

    def cached_(self, name, default=None):
        """ Get the cached (server) value of a field """
        if not self.persisted_:
            return default
        if name in self.__cache__:
            return self.__cache__[name]
        field = self.meta_.fields.get(name)
        # Need this redirection for Composite fields
        if field is not None:
            return field.get_cached_value(self)
        return getattr(self, name, default)

    def incr_(self, **kwargs):
        """ Atomically increment a number value """
        for key, val in six.iteritems(kwargs):
            if self._is_field_primary(key):
                raise AttributeError("Cannot increment an item's primary key!")

            field = self.meta_.fields.get(key)
            if field is not None:
                if field.ddb_data_type != NUMBER:
                    raise TypeError("Cannot increment non-number field '%s'" %
                                    key)
                if field.composite:
                    raise TypeError("Cannot increment composite field '%s'" %
                                    key)
            if key in self.__dirty__:
                raise ValueError("Cannot set field '%s' and increment it in "
                                 "the same update!" % key)
            self.__incrs__[key] = self.__incrs__.get(key, 0) + val
            if field is not None:
                self.__incrs__[key] = field.coerce(self.__incrs__[key], True)
                for name in self.meta_.related_fields[key]:
                    self.__cache__.setdefault(name, getattr(self, name))
                    if name != key:
                        self.__dirty__.add(name)
                self.__dict__[key] = self.cached_(key, 0) + self.__incrs__[key]
            else:
                self.__cache__.setdefault(key, getattr(self, key, 0))
                self._overflow[key] = self.cached_(
                    key, 0) + self.__incrs__[key]

    def add_(self, **kwargs):
        """ Atomically add to a set """
        self.mutate_('ADD', **kwargs)

    def remove_(self, **kwargs):
        """ Atomically remove from a set """
        self.mutate_('DELETE', **kwargs)

    def mutate_(self, action, **kwargs):
        """ Atomically mutate a set """
        for key, val in six.iteritems(kwargs):
            field = self.meta_.fields.get(key)
            if field is not None:
                if not field.is_set:
                    raise TypeError("Cannot mutate non-set field '%s'" %
                                    key)
                if field.composite:
                    raise TypeError("Cannot mutate composite field '%s'" %
                                    key)
            if key in self.__dirty__:
                raise ValueError("Cannot set field '%s' and mutate it in "
                                 "the same update!" % key)

            previous = self.__incrs__.get(key, SetDelta())
            previous.add(action, val)
            self.__incrs__[key] = previous
            if field is not None:
                for name in self.meta_.related_fields[key]:
                    self.__cache__.setdefault(name, getattr(self, name))
                    if name != key:
                        self.__dirty__.add(name)
                self.__dict__[key] = previous.merge(self.cached_(key))
            else:
                self.__cache__.setdefault(key, getattr(self, key, None))
                self._overflow[key] = previous.merge(self.cached_(key))

    def pre_save_(self, engine):
        """ Called before saving items """
        self.__engine__ = engine
        for field in six.itervalues(self.meta_.fields):
            field.validate(self)

    def post_save_fields_(self, fields):
        """ Called after update_field or update_fields """
        self.__dirty__.difference_update(fields)
        for name in fields:
            self.__incrs__.pop(name, None)
            if name in self.__cache__:
                self.__cache__[name] = copy.copy(getattr(self, name))

    def post_save_(self):
        """ Called after item is saved to database """
        self._persisted = True
        self.__dirty__ = set()
        self.__incrs__ = {}
        self._reset_cache()

    def post_load_(self, engine):
        """ Called after model loaded from database """
        if engine is not None:
            self.__engine__ = engine
        self._persisted = True
        self.__dirty__ = set()
        self.__incrs__ = {}
        self._reset_cache()

    def _reset_cache(self):
        """ Reset the __cache__ to only track mutable fields """
        self.__cache__ = {}
        for name in self.keys_():
            field = self.meta_.fields.get(name)
            if field is None:
                value = self.get_(name)
                if Field.is_overflow_mutable(value):
                    self.__cache__[name] = copy.copy(value)
            elif not field.composite and field.is_mutable:
                self.__cache__[name] = copy.copy(getattr(self, name))

    @contextlib.contextmanager
    def loading_(self, engine=None):
        """ Context manager to speed up object load process """
        self._loading = True
        self._overflow = {}
        yield
        self._loading = False
        self.post_load_(engine)

    @contextlib.contextmanager
    def partial_loading_(self):
        """ For use when loading a partial object (i.e. from update_field) """
        self._loading = True
        yield
        self._loading = False

    def ddb_dump_field_(self, name):
        """ Dump a field to a Dynamo-friendly value """
        val = getattr(self, name)
        if name in self.meta_.fields:
            return self.meta_.fields[name].ddb_dump(val)
        else:
            return Field.ddb_dump_overflow(val)

    def ddb_dump_(self):
        """ Return a dict for inserting into DynamoDB """
        data = {}
        for name in self.meta_.fields:
            data[name] = self.ddb_dump_field_(name)
        for name in self._overflow:
            data[name] = self.ddb_dump_field_(name)

        return data

    def set_ddb_val_(self, key, val):
        """ Decode and set a value retrieved from Dynamo """
        if key.startswith('_'):
            pass
        elif key in self.meta_.fields:
            setattr(self, key, self.meta_.fields[key].ddb_load(val))
        else:
            setattr(self, key, Field.ddb_load_overflow(val))

    @classmethod
    def ddb_load_(cls, engine, data):
        """ Load a model from DynamoDB data """
        obj = cls.__new__(cls)
        with obj.loading_(engine):
            for key, val in data.items():
                obj.set_ddb_val_(key, val)
        return obj

    def ddb_dump_cached_(self, name):
        """ Dump a cached field to a Dynamo-friendly value """
        val = self.cached_(name)
        if name in self.meta_.fields:
            return self.meta_.fields[name].ddb_dump(val)
        else:
            return Field.ddb_dump_overflow(val)

    def construct_ddb_expects_(self, fields=None):
        """ Construct a dynamo "expects" mapping based on the cached fields """
        if fields is None:
            fields = self.keys_()
        expect = {}
        for name in fields:
            val = self.ddb_dump_cached_(name)
            if val is None:
                expect[name + '__null'] = True
            else:
                expect[name + '__eq'] = val
        return expect

    @classmethod
    def field_(cls, name):
        """
        Get Field or construct a placeholder for an undeclared field

        This is used for creating scan filter constraints on fields that were
        not declared in the model

        """
        field = cls.meta_.fields.get(name)
        if field is not None:
            return field
        field = Field()
        field.name = name
        field.overflow = True
        return field

    def __json__(self, request=None):
        data = {}
        for name in self.meta_.fields:
            data[name] = getattr(self, name)
        for key, val in six.iteritems(self._overflow):
            data[key] = val
        return data

    def __hash__(self):
        return hash(self.hk_) + hash(self.rk_)

    def __eq__(self, other):
        return (isinstance(other, self.__class__) and
                self.meta_.name == other.meta_.name and
                self.hk_ == other.hk_ and
                self.rk_ == other.rk_)

    def __ne__(self, other):
        return not self.__eq__(other)
