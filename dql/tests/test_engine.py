""" Unit tests for the query engine """
from mock import MagicMock, patch, ANY
from . import TestCase
from ..engine import Engine
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
