""" Tests for the query engine """
from boto.dynamodb2.results import ResultSet
from mock import MagicMock, patch, ANY
from pyparsing import ParseException

from . import TestCase, BaseSystemTest
from ..engine import Engine, FragmentEngine
from ..models import TableMeta


class TestEngine(TestCase):

    """ Unit tests for the query engine """

    def setUp(self):
        super(TestEngine, self).setUp()
        self.table = patch('dql.engine.Table').start()()
        self.describe = patch('dql.engine.Engine.describe').start()
        self.engine = Engine(MagicMock())

    def tearDown(self):
        super(TestEngine, self).tearDown()
        patch.stopall()

    def test_select_consistent(self):
        """ SELECT can make a consistent read """
        self.engine.execute("SELECT CONSISTENT * FROM foobar WHERE id = 'a'")
        self.table.query.assert_called_with(id__eq='a', consistent=True)

    def test_select_in_consistent(self):
        """ SELECT by primary key can make a consistent read """
        self.describe.return_value = TableMeta('', '', '', MagicMock(),
                                               MagicMock(), 0, [], 1, 1, 0)
        self.engine.execute("SELECT CONSISTENT * FROM foobar "
                            "WHERE KEYS IN ('a', 1)")
        self.table.batch_get.assert_called_with(keys=ANY, consistent=True)

    def test_count_consistent(self):
        """ COUNT can make a consistent read """
        self.describe.return_value = TableMeta('', '', '', MagicMock(),
                                               MagicMock(), 0, [], 1, 1, 0)
        self.engine.execute("count CONSISTENT foobar WHERE id = 'a'")
        self.table.query_count.assert_called_with(id__eq='a', consistent=True)


class TestEngineSystem(BaseSystemTest):

    """ System tests for the Engine """

    def test_variables(self):
        """ Statements can use variables instead of string/number literals """
        self.make_table()
        self.query("INSERT INTO foobar (id, bar) VALUES (id, 5)",
                   scope={'id': 'a'})
        results = self.query("SCAN foobar")
        results = [dict(r) for r in results]
        self.assertItemsEqual(results, [{'id': 'a', 'bar': 5}])

    def test_missing_var(self):
        """ If a variable is missing it raises a NameError """
        self.make_table()
        self.assertRaises(NameError, self.query,
                          "INSERT INTO foobar (id, bar) VALUES (id, 5)")

    def test_insert_float(self):
        """ Inserting a float doesn't cause serialization issues """
        self.make_table()
        self.query("INSERT INTO foobar (id, bar) VALUES ('a', 5.284)")

    def test_insert_float_from_var(self):
        """ Inserting a float from a var doesn't cause serialization issues """
        self.make_table()
        self.query("INSERT INTO foobar (id, bar) VALUES ('a', bar)", scope={'bar': 1.234})


class TestFragmentEngine(BaseSystemTest):

    """ Tests for the FragmentEngine """

    def setUp(self):
        super(TestFragmentEngine, self).setUp()
        self.engine = FragmentEngine(self.dynamo)

    def test_no_run_fragment(self):
        """ Engine should not run query fragments """
        result = self.query("SELECT * FROM table WHERE")
        self.assertIsNone(result)

    def test_no_run_multi_fragment(self):
        """ A complete statement that ends in a fragment should not run """
        self.query("SELECT * FROM table WHERE")
        result = self.query("foo = 'bar'; DROP")
        self.assertIsNone(result)

    def test_run_query(self):
        """ If fragments add up to a query, it should run """
        self.query("CREATE TABLE test ")
        self.query("(id STRING ")
        result = self.query("HASH KEY);")
        self.assertIsNotNone(result)
        desc = self.engine.describe('test')
        self.assertIsNotNone(desc)

    def test_format_exc(self):
        """ Fragment engine can pretty-format a parse error """
        query = "SELECT * FROM\n\ntable WHERE\n;"
        try:
            for fragment in query.split('\n'):
                self.query(fragment)
        except ParseException as e:
            pretty = self.engine.pformat_exc(e)
            self.assertEquals(pretty, query + '\n' +
                              '^\n' +
                              "Expected variable (at char 27), (line:4, col:1)")
        else:
            assert False, "Engine should raise exception if parsing fails"

    def test_preserve_whitespace(self):
        """ The engine should preserve the whitespace between fragments """
        query = "DUMP\nSCHEMA\n\n;"
        for fragment in query.split('\n'):
            self.query(fragment)
        self.assertEquals(self.engine.last_query, query)
