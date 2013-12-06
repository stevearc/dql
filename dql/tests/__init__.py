""" Testing tools for DQL """
import os

import inspect
import logging
import nose
import subprocess
import tempfile
from boto.dynamodb2.layer1 import DynamoDBConnection
from urllib import urlretrieve


DYNAMO_LOCAL = 'https://s3-us-west-2.amazonaws.com/dynamodb-local/dynamodb_local_2013-09-12.tar.gz'


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

    def options(self, parser, env):
        super(DynamoLocalPlugin, self).options(parser, env)
        parser.add_option('--dynamo-port', type=int, default=8000,
                          help="Run the Dynamo Local service on this port "
                          "(default %(default)s)")
        parser.add_option('--dynamo-path', help="Download the Dynamo Local "
                          "server to this directory")
        parser.add_option('--dynamo-link', default=DYNAMO_LOCAL,
                          help="The link to the dynamodb local server code "
                          "(default %(default)s)")

    def configure(self, options, conf):
        super(DynamoLocalPlugin, self).configure(options, conf)
        self.port = options.dynamo_port
        self.path = options.dynamo_path
        self.link = options.dynamo_link
        if self.path is None:
            self.path = os.path.join(tempfile.gettempdir(), 'dynamolocal')
        logging.getLogger('boto').setLevel(logging.WARNING)

    @property
    def dynamo(self):
        """ Lazy loading of the dynamo connection """
        if self._dynamo is None:
            if not os.path.exists(self.path):
                tarball = urlretrieve(self.link)[0]
                subprocess.check_call(['tar', '-zxf', tarball])
                name = os.path.basename(self.link).split('.')[0]
                os.rename(name, self.path)
                os.unlink(tarball)

            cmd = ['java', '-Djava.library.path=' + self.path, '-jar',
                   os.path.join(self.path, 'DynamoDBLocal.jar'), '--port',
                   str(self.port)]
            self._dynamo_local = subprocess.Popen(cmd)
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
