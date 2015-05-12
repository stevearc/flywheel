""" Test backwards-compatible behavior """
import json

from flywheel import Field, Model
from flywheel.fields.types import TypeDefinition, DictType, STRING
from flywheel.tests import DynamoSystemTest


class JsonType(TypeDefinition):

    """ Simple type that serializes to JSON """

    data_type = json
    ddb_data_type = STRING

    def coerce(self, value, force):
        return value

    def ddb_dump(self, value):
        return json.dumps(value)

    def ddb_load(self, value):
        return json.loads(value)


class OldDict(Model):

    """ Model that uses an old-style json field as a dict store """

    __metadata__ = {
        '_name': 'dict-test',
    }

    id = Field(hash_key=True)
    data = Field(data_type=JsonType())


class TestOldJsonTypes(DynamoSystemTest):

    """ Test the graceful handling of old json-serialized data """

    models = [OldDict]

    def setUp(self):
        super(TestOldJsonTypes, self).setUp()
        OldDict.meta_.fields['data'].data_type = JsonType()

    def test_migrate_data(self):
        """ Test graceful load of old json-serialized data """
        old = OldDict('a', data={'a': 1})
        self.engine.save(old)
        OldDict.meta_.fields['data'].data_type = DictType()
        new = self.engine.scan(OldDict).one()
        self.assertEqual(new.data, old.data)

    def test_resave_old_data(self):
        """ Test the resaving of data that used to be json """
        old = OldDict('a', data={'a': 1})
        self.engine.save(old)
        OldDict.meta_.fields['data'].data_type = DictType()
        new = self.engine.scan(OldDict).one()
        new.data['b'] = 2
        new.sync(raise_on_conflict=False)
        ret = self.engine.scan(OldDict).one()
        self.assertEqual(ret.data, {'a': 1, 'b': 2})
