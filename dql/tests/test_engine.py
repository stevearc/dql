""" Unit tests for the query engine """
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


class TestFragmentEngine(BaseSystemTest):

    """ Tests for the FragmentEngine """

    def setUp(self):
        super(TestFragmentEngine, self).setUp()
        self.engine = FragmentEngine(self.dynamo)

    def test_no_run_fragment(self):
        """ Engine should not run query fragments """
        result = self.engine.execute("SELECT * FROM table WHERE")
        self.assertIsNone(result)

    def test_no_run_multi_fragment(self):
        """ A complete statement that ends in a fragment should not run """
        self.engine.execute("SELECT * FROM table WHERE")
        result = self.engine.execute("foo = 'bar'; DROP")
        self.assertIsNone(result)

    def test_run_query(self):
        """ If fragments add up to a query, it should run """
        self.engine.execute("CREATE TABLE test ")
        self.engine.execute("(id STRING ")
        result = self.engine.execute("HASH KEY);")
        self.assertIsNotNone(result)
        desc = self.engine.describe('test')
        self.assertIsNotNone(desc)

    def test_format_exc(self):
        """ Fragment engine can pretty-format a parse error """
        query = "SELECT * FROM table WHERE thisisaproblem;"
        try:
            self.engine.execute(query)
        except ParseException as e:
            pretty = self.engine.pformat_exc(e)
            self.assertEquals(pretty, query + '\n' +
                              40 * ' ' + '^\n' +
                              "Expected '=' (at char 40), (line:1, col:41)")
        else:
            assert False, "Engine should raise exception if parsing fails"
