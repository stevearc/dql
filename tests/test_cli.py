""" Tests for the CLI """
import json
import shutil
import tempfile
import unittest
from base64 import b64encode
from collections.abc import Iterable
from io import BytesIO, StringIO, TextIOWrapper
from typing import Any, List
from urllib.parse import urlparse

from dynamo3 import DynamoDBConnection
from mock import patch
from snapshottest import TestCase

from dql.cli import DQLClient, repl_command

from . import BaseSystemTest


class UniqueCollection(object):
    """ Wrapper to make equality tests simpler """

    def __init__(self, items):
        self._items = set(items)

    def __repr__(self):
        return repr(self._items)

    def __eq__(self, other):
        return isinstance(other, Iterable) and self._items == set(other)

    def __ne__(self, other):
        return not self.__eq__(other)


class BaseCLITest(TestCase):

    """ Base class for CLI tests """

    dynamo: DynamoDBConnection = None
    cli: DQLClient
    confdir: str
    patcher: Any

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.cli = DQLClient()
        cls.confdir = tempfile.mkdtemp()
        host = urlparse(cls.dynamo.host)
        cls.cli.initialize(host=host.hostname, port=host.port, config_dir=cls.confdir)
        # Have to patch this so we don't make requests to CloudWatch
        cls.patcher = patch("dql.engine.Engine._get_metric", spec=True)
        method = cls.patcher.start()
        method.return_value = 0

    @classmethod
    def tearDownClass(cls):
        super().tearDownClass()
        shutil.rmtree(cls.confdir)
        cls.patcher.stop()

    def setUp(self):
        super().setUp()
        # Clear out any pre-existing tables
        conn = self.cli.engine.connection
        for tablename in conn.list_tables():
            conn.delete_table(tablename, wait=True)

    def tearDown(self):
        super().tearDown()
        conn = self.cli.engine.connection
        for tablename in conn.list_tables():
            conn.delete_table(tablename, wait=True)


class TestCli(BaseCLITest):

    """ Tests for the CLI """

    def assert_prints(self, command, message):
        """ Assert that a cli command will print a message to the console """
        out = StringIO()
        with patch("sys.stdout", out):
            self.cli.onecmd(command)
        self.assertEqual(out.getvalue().strip(), message.strip())

    def test_repl_command_args(self):
        """ The @repl_command decorator parses arguments and passes them in """

        @repl_command
        def testfunc(zelf, first, second):
            """ Test cli command """
            self.assertEqual(zelf, self)
            self.assertEqual(first, "a")
            self.assertEqual(second, "b")

        testfunc(self, "a b")  # pylint: disable=E1120

    def test_repl_command_kwargs(self):
        """ The @repl_command decorator parses kwargs and passes them in """

        @repl_command
        def testfunc(zelf, first, second=None):
            """ Test cli command """
            self.assertEqual(zelf, self)
            self.assertEqual(first, "a")
            self.assertEqual(second, "b")

        testfunc(self, "a second=b")

    def test_help_docs(self):
        """ There is a help command for every DQL query type """
        import dql.help

        for name in dir(dql.help):
            # Options is not a query type
            if name == "OPTIONS":
                continue
            if not name.startswith("_"):
                self.assert_prints("help %s" % name.lower(), getattr(dql.help, name))


class TestCliCommands(BaseCLITest):

    """ Tests that run the 'dql --command' """

    def _run_command(self, command: str) -> str:
        stream = BytesIO()
        out = TextIOWrapper(stream)
        with patch("sys.stdout", out):
            self.cli.run_command(command, use_json=True, raise_exceptions=True)
        self.assertFalse(self.cli.engine.partial, "Command was not terminated properly")
        out.seek(0)
        return out.read()

    def _run_dql_command(self, command: str) -> List[Any]:
        output = self._run_command(command)
        ret: List[Any] = []
        for line in output.split("\n"):
            if not line:
                continue
            try:
                ret.append(json.loads(line))
            except json.JSONDecodeError:
                print("Total output: %s" % output)
                print("Error decoding json: %r" % line)
                self.fail()
        return ret

    def test_scan_table(self):
        """ Can create, insert, and scan from table """
        lines = self._run_dql_command(
            """
        CREATE TABLE foobar (id STRING HASH KEY);
        INSERT INTO foobar (id='a', num=1, bin=b'a',
            ss=('a1', 'a2'), ns=(1, 2), bs=(b'a1', b'a2'),
            list=[1, 'a'],
            dict={'a': 1, 'b': 'c'},
            bool=TRUE
        );
        SCAN * FROM foobar;
        """
        )
        self.assertEqual(len(lines), 1)
        item = lines[0]
        self.assertEqual(
            item,
            {
                "id": "a",
                "num": 1,
                "bin": b64encode(b"a").decode("ascii"),
                # Sets will be converted to lists in json
                "ss": UniqueCollection(["a1", "a2"]),
                "ns": UniqueCollection([1, 2]),
                "bs": UniqueCollection(
                    [b64encode(b"a1").decode("ascii"), b64encode(b"a2").decode("ascii")]
                ),
                "list": [1, "a"],
                "dict": {"a": 1, "b": "c"},
                "bool": True,
            },
        )

    def test_ls(self):
        """ Snapshot test for ls format """
        self._run_dql_command(
            """
        CREATE TABLE foobar (
            id STRING HASH KEY,
            range NUMBER RANGE KEY,
            foo STRING INDEX('foo-index')
        ) GLOBAL INDEX ('bar-index', bar STRING);
        """
        )
        output = self._run_command("ls foobar")
        self.assertMatchSnapshot(output)

    def test_ls_with_multiple_tables(self):
        """ Snapshot test for ls format """
        self._run_dql_command(
            """
        CREATE TABLE foo (
            id STRING HASH KEY,
            range NUMBER RANGE KEY,
            foo STRING INDEX('foo-index')
        );
        CREATE TABLE bar (
            id STRING HASH KEY,
            range NUMBER RANGE KEY,
            bar STRING INDEX('bar-index')
        );
        """
        )
        output = self._run_command("ls")
        self.assertMatchSnapshot(output)
