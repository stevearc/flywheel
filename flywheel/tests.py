""" Unit and system tests for flywheel """
import os

import inspect
import logging
import nose
import shutil
import subprocess
import tempfile
from boto.dynamodb2 import connect_to_region
from boto.dynamodb2.layer1 import DynamoDBConnection
from flywheel.engine import Engine
from urllib import urlretrieve


try:
    import unittest2 as unittest  # pylint: disable=F0401
except ImportError:
    import unittest


DYNAMO_LOCAL = 'https://s3-us-west-2.amazonaws.com/dynamodb-local/dynamodb_local_2013-12-12.tar.gz'


class DynamoLocalPlugin(nose.plugins.Plugin):

    """
    Nose plugin to run the Dynamo Local service

    See: http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Tools.html

    """
    name = 'dynamolocal'

    def __init__(self):
        super(DynamoLocalPlugin, self).__init__()
        self._dynamo_local = None
        self._dynamo = None
        self.port = None
        self.path = None
        self.link = None
        self.java = None
        self.region = None

    def options(self, parser, env):
        super(DynamoLocalPlugin, self).options(parser, env)
        tempdir = os.path.join(tempfile.gettempdir(), 'dynamolocal')
        parser.add_option('--dynamo-port', type=int, default=8000,
                          help="Run the Dynamo Local service on this port "
                          "(default 8000)")
        parser.add_option('--dynamo-path', default=tempdir,
                          help="Download the Dynamo Local server to this "
                          "directory (default %s)" % tempdir)
        parser.add_option('--dynamo-link', default=DYNAMO_LOCAL,
                          help="The link to the dynamodb local server code "
                          "(default %s)" % DYNAMO_LOCAL)
        parser.add_option('--dynamo-java', default='java',
                          help="The path to the java executable (default java)")
        parser.add_option('--dynamo-region', help="If provided, tests will "
                          "create tables in this LIVE dynamo region "
                          "instead of using DynamoDB Local. USE WITH CAUTION.")

    def configure(self, options, conf):
        super(DynamoLocalPlugin, self).configure(options, conf)
        self.port = options.dynamo_port
        self.path = options.dynamo_path
        self.link = options.dynamo_link
        self.java = options.dynamo_java
        self.region = options.dynamo_region
        logging.getLogger('boto').setLevel(logging.WARNING)

    @property
    def dynamo(self):
        """ Lazy loading of the dynamo connection """
        if self._dynamo is None:
            if self.region is not None:
                self._dynamo = connect_to_region(self.region)
            else:
                if not os.path.exists(self.path):
                    tarball = urlretrieve(self.link)[0]
                    subprocess.check_call(['tar', '-zxf', tarball])
                    name = os.path.basename(self.link).split('.')[0]
                    shutil.move(name, self.path)
                    os.unlink(tarball)

                lib_path = os.path.join(self.path, 'DynamoDBLocal_lib')
                jar_path = os.path.join(self.path, 'DynamoDBLocal.jar')
                cmd = [self.java, '-Djava.library.path=' + lib_path, '-jar',
                       jar_path, '--port', str(self.port)]
                self._dynamo_local = subprocess.Popen(cmd,
                                                      stdout=subprocess.PIPE,
                                                      stderr=subprocess.STDOUT)
                self._dynamo = DynamoDBConnection(
                    host='localhost',
                    port=self.port,
                    is_secure=False,
                    aws_access_key_id='',
                    aws_secret_access_key='')
        return self._dynamo

    def startContext(self, context):  # pylint: disable=C0103
        """ Called at the beginning of modules and TestCases """
        # If this is a TestCase, dynamically set the dynamo connection
        if inspect.isclass(context) and hasattr(context, 'dynamo'):
            context.dynamo = self.dynamo

    def finalize(self, result):
        """ terminate the dynamo local service """
        if self._dynamo_local is not None:
            self._dynamo_local.terminate()
            if not result.wasSuccessful():
                print self._dynamo_local.stdout.read()


class DynamoSystemTest(unittest.TestCase):

    """ Base class for tests that need an :class:`~flywheel.engine.Engine` """
    dynamo = None
    models = []

    @classmethod
    def setUpClass(cls):
        super(DynamoSystemTest, cls).setUpClass()
        logging.getLogger('boto').setLevel(logging.WARNING)
        cls.engine = Engine(cls.dynamo, ['test'])
        cls.engine.register(*cls.models)
        cls.engine.create_schema()

    @classmethod
    def tearDownClass(cls):
        super(DynamoSystemTest, cls).tearDownClass()
        cls.engine.delete_schema()

    def tearDown(self):
        super(DynamoSystemTest, self).tearDown()
        for model in self.engine.models.itervalues():
            self.engine.scan(model).delete()
