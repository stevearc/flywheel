""" Tests for schema changes. """
from dynamo3 import Throughput
from flywheel import (Field, NUMBER, GlobalIndex)
from flywheel.models import Model
from flywheel.tests import DynamoSystemTest


# pylint: disable=C0121

class WidgetToAddIndex(Model):
    """ Model for testing default field values """
    __metadata__ = {
        'global_indexes': [
            GlobalIndex('gindex', 'string2', 'num'),
        ],
    }
    string = Field(hash_key=True)
    string2 = Field()
    num = Field(data_type=NUMBER)

    def __init__(self, string, string2, num):
        super(WidgetToAddIndex, self).__init__()
        self.string = string
        self.string2 = string2
        self.num = num


class TestAddIndex(DynamoSystemTest):
    """ Tests index updates. """
    dynamo = None
    models = [WidgetToAddIndex]

    def setUp(self):
        # Forcing the engine to reset.
        self.setUpClass()
        super(TestAddIndex, self).setUp()

    def tearDown(self):
        super(TestAddIndex, self).tearDown()
        # Clearing the engine on each test.
        self.tearDownClass()

        # resetting the Widget.
        WidgetToAddIndex.meta_.global_indexes = [WidgetToAddIndex.meta_.global_indexes[0].throughput(read=5, write=5)]
        WidgetToAddIndex.meta_.post_create()
        WidgetToAddIndex.meta_.validate_model()
        WidgetToAddIndex.meta_.post_validate()

    def test_add_index(self):
        """Simulates adding a global secondary index at a later time."""
        table = self.engine.dynamo.describe_table(WidgetToAddIndex.meta_.ddb_tablename(self.engine.namespace))
        self.assertEqual(len(table.global_indexes), 1)

        # Simulating adding an index later on.
        WidgetToAddIndex.meta_.global_indexes.append(GlobalIndex('gindex-2', 'string2', 'string'))
        WidgetToAddIndex.meta_.post_create()
        WidgetToAddIndex.meta_.validate_model()
        WidgetToAddIndex.meta_.post_validate()

        changed = self.engine.update_schema()
        self.assertListEqual(changed, [WidgetToAddIndex.meta_.ddb_tablename(self.engine.namespace)])

        table = self.engine.dynamo.describe_table(WidgetToAddIndex.meta_.ddb_tablename(self.engine.namespace))
        self.assertEqual(len(table.global_indexes), 2)

        one = WidgetToAddIndex("one-1", "one-2", 1)
        two = WidgetToAddIndex("one-2", "test", 2)

        self.engine.save(one)
        self.engine.save(two)

        result = self.engine.query(WidgetToAddIndex).index('gindex-2').filter(WidgetToAddIndex.string2 == 'test').all()

        self.assertEqual(len(result), 1)
        self.assertNotEqual(result[0], one)
        self.assertEqual(result[0], two)

    def test_alter_throughput_from_defaults(self):
        """ Updates index throughput values by comparing the current values to those from the actual database.

        This might not be a great idea. create_schema doesn't seem to perform any updates with read/write
        throughput capacity changes. Not really sure if this should.

        """
        table = self.engine.dynamo.describe_table(WidgetToAddIndex.meta_.ddb_tablename(self.engine.namespace))
        self.assertEqual(len(table.global_indexes), 1)
        self.assertEqual(table.global_indexes[0].throughput, Throughput(5, 5))

        # Simulating adding an index later on.
        WidgetToAddIndex.meta_.global_indexes[0] = WidgetToAddIndex.meta_.global_indexes[0].throughput(read=10,
                                                                                                       write=11)
        WidgetToAddIndex.meta_.post_create()
        WidgetToAddIndex.meta_.validate_model()
        WidgetToAddIndex.meta_.post_validate()

        changed = self.engine.update_schema()
        self.assertListEqual(changed, [WidgetToAddIndex.meta_.ddb_tablename(self.engine.namespace)])

        table = self.engine.dynamo.describe_table(WidgetToAddIndex.meta_.ddb_tablename(self.engine.namespace))
        self.assertEqual(table.global_indexes[0].throughput, Throughput(10, 11))
        self.assertEqual(len(table.global_indexes), 1)

        one = WidgetToAddIndex("one-1", "one-2", 1)
        two = WidgetToAddIndex("one-2", "test", 2)

        self.engine.save(one)
        self.engine.save(two)

        result = self.engine.query(WidgetToAddIndex).index('gindex').filter(WidgetToAddIndex.string2 == 'test').all()

        self.assertEqual(len(result), 1)
        self.assertNotEqual(result[0], one)
        self.assertEqual(result[0], two)

    def test_alter_throughput_directly(self):
        """ Tests that throughput provisioning specified directly at update time are applied to the index.

        """
        table = self.engine.dynamo.describe_table(WidgetToAddIndex.meta_.ddb_tablename(self.engine.namespace))
        self.assertEqual(len(table.global_indexes), 1)
        self.assertEqual(table.global_indexes[0].throughput, Throughput(5, 5))

        changed = self.engine.update_schema(throughput={
            WidgetToAddIndex.meta_.ddb_tablename(): {
                "gindex": {
                    "read": 12,
                    "write": 13
                }}})
        self.assertListEqual(changed, [WidgetToAddIndex.meta_.ddb_tablename(self.engine.namespace)])

        table = self.engine.dynamo.describe_table(WidgetToAddIndex.meta_.ddb_tablename(self.engine.namespace))
        self.assertEqual(table.global_indexes[0].throughput, Throughput(12, 13))
        self.assertEqual(len(table.global_indexes), 1)

        one = WidgetToAddIndex("one-1", "one-2", 1)
        two = WidgetToAddIndex("one-2", "test", 2)

        self.engine.save(one)
        self.engine.save(two)

        result = self.engine.query(WidgetToAddIndex).index('gindex').filter(WidgetToAddIndex.string2 == 'test').all()

        self.assertEqual(len(result), 1)
        self.assertNotEqual(result[0], one)
        self.assertEqual(result[0], two)
