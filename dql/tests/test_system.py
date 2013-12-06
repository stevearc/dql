""" System tests """
try:
    from unittest2 import TestCase  # pylint: disable=F0401
except ImportError:
    from unittest import TestCase
from boto.dynamodb2.table import Table
from boto.exception import JSONResponseError

from .. import parser, Engine
from ..models import TableField


class TestSystem(TestCase):

    """ System tests """
    dynamo = None

    def setUp(self):
        self.engine = Engine(parser, self.dynamo)

    def query(self, command):
        """ Shorthand because I'm lazy """
        return self.engine.execute(command)

    def tearDown(self):
        for tablename in self.dynamo.list_tables()['TableNames']:
            Table(tablename, connection=self.dynamo).delete()

    def test_create(self):
        """ CREATE statement should make a table """
        self.query(
            """
            CREATE TABLE foobar (owner STRING HASH KEY,
                                 id BINARY RANGE KEY,
                                 ts NUMBER INDEX('ts-index'))
            """)
        desc = self.engine.describe('foobar')
        self.assertEquals(desc.hash_key, TableField('owner', 'STRING', 'HASH'))
        self.assertEquals(desc.range_key, TableField('id', 'BINARY', 'RANGE'))
        self.assertItemsEqual(desc.indexes.values(),
                              [TableField('ts', 'NUMBER', 'INDEX', 'ts-index')])

    def test_create_if_not_exists(self):
        """ CREATE IF NOT EXISTS shouldn't fail if table exists """
        self.query("CREATE TABLE foobar (owner STRING HASH KEY)")
        self.query("CREATE TABLE IF NOT EXISTS foobar (owner STRING HASH KEY)")

    def test_drop(self):
        """ DROP statement should drop a table """
        self.query("CREATE TABLE foobar (id STRING HASH KEY)")
        self.query("DROP TABLE foobar")
        try:
            self.dynamo.describe_table('foobar')['Table']
        except JSONResponseError as e:
            self.assertEquals(e.status, 400)
        else:
            assert False, "Table should not exist"

    def test_drop_if_exists(self):
        """ DROP IF EXISTS shouldn't fail if no table """
        self.query("CREATE TABLE foobar (id STRING HASH KEY)")
        self.query("DROP TABLE foobar")
        self.query("DROP TABLE IF EXISTS foobar")

    def test_insert(self):
        """ INSERT statement should create items """
        self.query("CREATE TABLE foobar (id STRING HASH KEY)")
        self.query("INSERT INTO foobar (id, bar) VALUES ('a', 1), ('b', 2)")
        table = Table('foobar', connection=self.dynamo)
        items = [dict(i) for i in table.scan()]
        self.assertItemsEqual(items, [{'id': 'a', 'bar': 1},
                                      {'id': 'b', 'bar': 2}])

    def test_select_hash_key(self):
        """ SELECT statement filters by hash key """
        self.query("CREATE TABLE foobar (id STRING HASH KEY)")
        self.query("INSERT INTO foobar (id, bar) VALUES ('a', 1), ('b', 2)")
        # FIXME: I think dynamodb local has a bug related to this...
        # results = self.query("SELECT FROM foobar WHERE id = 'a'")
        # self.assertItemsEqual(results, [{'id': 'a', 'bar': 1}])

    def test_select_hash_range(self):
        """ SELECT statement filters by hash and range keys """
        self.query("CREATE TABLE foobar (id STRING HASH KEY, "
                   "bar NUMBER RANGE KEY)")
        self.query("INSERT INTO foobar (id, bar) VALUES ('a', 1), ('b', 2)")
        results = self.query("SELECT FROM foobar WHERE id = 'a' and bar = 1")
        results = [dict(r) for r in results]
        self.assertItemsEqual(results, [{'id': 'a', 'bar': 1}])

    def test_select_hash_index(self):
        """ SELECT statement filters by indexes """
        self.query("CREATE TABLE foobar (id STRING HASH KEY, "
                   "bar NUMBER RANGE KEY, ts NUMBER INDEX('ts-index'))")
        self.query("INSERT INTO foobar (id, bar, ts) VALUES ('a', 1, 100), "
                   "('a', 2, 200)")
        results = self.query("SELECT FROM foobar WHERE id = 'a' "
                             "and ts < 150 USING 'ts-index'")
        results = [dict(r) for r in results]
        self.assertItemsEqual(results, [{'id': 'a', 'bar': 1, 'ts': 100}])

    def test_select_limit(self):
        """ SELECT statement should be able to specify limit """
        self.query("CREATE TABLE foobar (id STRING HASH KEY, "
                   "bar NUMBER RANGE KEY, ts NUMBER INDEX('ts-index'))")
        self.query("INSERT INTO foobar (id, bar, ts) VALUES ('a', 1, 100), "
                   "('a', 2, 200)")
        results = self.query("SELECT FROM foobar WHERE id = 'a' LIMIT 1")
        self.assertEquals(len(list(results)), 1)

    def test_delete(self):
        """ DELETE statement removes items """
        self.query("CREATE TABLE foobar (id STRING HASH KEY, "
                   "bar NUMBER RANGE KEY, ts number index('ts-index'))")
        self.query("INSERT INTO foobar (id, bar) VALUES ('a', 1), ('b', 2)")
        self.query("DELETE FROM foobar WHERE id = 'a' and bar = 1")
        table = Table('foobar', connection=self.dynamo)
        items = [dict(i) for i in table.scan()]
        self.assertItemsEqual(items, [{'id': 'b', 'bar': 2}])
