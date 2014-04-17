""" Unit and system tests for flywheel """
import six
from flywheel.engine import Engine


try:
    import unittest2 as unittest  # pylint: disable=F0401
except ImportError:
    import unittest


class DynamoSystemTest(unittest.TestCase):

    """ Base class for tests that need an :class:`~flywheel.engine.Engine` """
    dynamo = None
    models = []

    @classmethod
    def setUpClass(cls):
        super(DynamoSystemTest, cls).setUpClass()
        cls.engine = Engine(cls.dynamo, ['test'])
        cls.engine.register(*cls.models)
        cls.engine.create_schema()

    @classmethod
    def tearDownClass(cls):
        super(DynamoSystemTest, cls).tearDownClass()
        cls.engine.delete_schema()

    def tearDown(self):
        super(DynamoSystemTest, self).tearDown()
        for model in six.itervalues(self.engine.models):
            self.engine.scan(model).delete()
