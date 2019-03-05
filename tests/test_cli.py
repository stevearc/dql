""" Tests for the CLI """
import shutil
import tempfile
from future.moves.urllib.parse import urlparse
from io import StringIO
from mock import patch

from dql.cli import repl_command, DQLClient


try:
    import unittest2 as unittest  # pylint: disable=F0401
except ImportError:
    import unittest


class TestCli(unittest.TestCase):

    """ Tests for the CLI """
    dynamo = None

    def setUp(self):
        super(TestCli, self).setUp()
        self.confdir = tempfile.mkdtemp()
        self.cli = DQLClient()
        host = urlparse(self.dynamo.host)
        self.cli.initialize('local', port=host.port, config_dir=self.confdir)

    def tearDown(self):
        super(TestCli, self).tearDown()
        shutil.rmtree(self.confdir)

    def assert_prints(self, command, message):
        """ Assert that a cli command will print a message to the console """
        out = StringIO()
        with patch('sys.stdout', out):
            self.cli.onecmd(command)
        self.assertEqual(out.getvalue().strip(), message.strip())

    def test_repl_command_args(self):
        """ The @repl_command decorator parses arguments and passes them in """
        @repl_command
        def testfunc(zelf, first, second):
            """ Test cli command """
            self.assertEqual(zelf, self)
            self.assertEqual(first, 'a')
            self.assertEqual(second, 'b')
        testfunc(self, 'a b')  # pylint: disable=E1120

    def test_repl_command_kwargs(self):
        """ The @repl_command decorator parses kwargs and passes them in """
        @repl_command
        def testfunc(zelf, first, second=None):
            """ Test cli command """
            self.assertEqual(zelf, self)
            self.assertEqual(first, 'a')
            self.assertEqual(second, 'b')
        testfunc(self, 'a second=b')

    def test_help_docs(self):
        """ There is a help command for every DQL query type """
        import dql.help
        for name in dir(dql.help):
            # Options is not a query type
            if name == 'OPTIONS':
                continue
            if not name.startswith('_'):
                self.assert_prints('help %s' % name.lower(), getattr(dql.help, name))
