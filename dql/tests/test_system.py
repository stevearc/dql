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
        # Clear out any pre-existing tables
        for tablename in self.dynamo.list_tables()['TableNames']:
            Table(tablename, connection=self.dynamo).delete()

    def query(self, command):
        """ Shorthand because I'm lazy """
        return self.engine.execute(command)

    def tearDown(self):
        for tablename in self.dynamo.list_tables()['TableNames']:
            Table(tablename, connection=self.dynamo).delete()

    def make_table(self, name='foobar', hash_key='id', range_key='bar',
                   index=None):
        """ Shortcut for making a simple table """
        rng = ''
        if range_key is not None:
            rng = ",%s NUMBER RANGE KEY" % range_key
        idx = ''
        if index is not None:
            idx = ",{0} NUMBER INDEX('{0}-index')".format(index)
        self.query("CREATE TABLE %s (%s STRING HASH KEY %s%s)" %
                   (name, hash_key, rng, idx))
        return Table(name, connection=self.dynamo)

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

    def test_create_throughput(self):
        """ CREATE statement can specify throughput """
        self.query("CREATE TABLE foobar (id STRING HASH KEY) THROUGHPUT (1, 2)")
        desc = self.engine.describe('foobar')
        self.assertEquals(desc.read_throughput, 1)
        self.assertEquals(desc.write_throughput, 2)

    def test_alter_throughput(self):
        """ Can alter throughput of a table """
        self.query("CREATE TABLE foobar (id STRING HASH KEY) THROUGHPUT (1, 1)")
        self.query("ALTER TABLE foobar SET THROUGHPUT (2, 2)")
        desc = self.engine.describe('foobar')
        self.assertEquals(desc.read_throughput, 2)
        self.assertEquals(desc.write_throughput, 2)

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
        table = self.make_table()
        self.query("INSERT INTO foobar (id, bar) VALUES ('a', 1), ('b', 2)")
        items = [dict(i) for i in table.scan()]
        self.assertItemsEqual(items, [{'id': 'a', 'bar': 1},
                                      {'id': 'b', 'bar': 2}])

    def test_select_hash_key(self):
        """ SELECT statement filters by hash key """
        self.make_table()
        self.query("INSERT INTO foobar (id, bar) VALUES ('a', 1), ('b', 2)")
        # FIXME: I think dynamodb local has a bug related to this...
        # results = self.query("SELECT * FROM foobar WHERE id = 'a'")
        # self.assertItemsEqual(results, [{'id': 'a', 'bar': 1}])

    def test_select_hash_range(self):
        """ SELECT statement filters by hash and range keys """
        self.make_table()
        self.query("INSERT INTO foobar (id, bar) VALUES ('a', 1), ('b', 2)")
        results = self.query("SELECT * FROM foobar WHERE id = 'a' and bar = 1")
        results = [dict(r) for r in results]
        self.assertItemsEqual(results, [{'id': 'a', 'bar': 1}])

    def test_select_get(self):
        """ SELECT statement can fetch items directly """
        self.make_table()
        self.query("INSERT INTO foobar (id, bar) VALUES ('a', 1), ('b', 2)")
        results = self.query("SELECT * FROM foobar WHERE "
                             "KEYS IN ('a', 1), ('b', 2)")
        results = [dict(r) for r in results]
        self.assertItemsEqual(results, [{'id': 'a', 'bar': 1},
                                        {'id': 'b', 'bar': 2}])

    def test_select_hash_index(self):
        """ SELECT statement filters by indexes """
        self.make_table(index='ts')
        self.query("INSERT INTO foobar (id, bar, ts) VALUES ('a', 1, 100), "
                   "('a', 2, 200)")
        results = self.query("SELECT * FROM foobar WHERE id = 'a' "
                             "and ts < 150 USING 'ts-index'")
        results = [dict(r) for r in results]
        self.assertItemsEqual(results, [{'id': 'a', 'bar': 1, 'ts': 100}])

    def test_select_limit(self):
        """ SELECT statement should be able to specify limit """
        self.make_table(index='ts')
        self.query("INSERT INTO foobar (id, bar, ts) VALUES ('a', 1, 100), "
                   "('a', 2, 200)")
        results = self.query("SELECT * FROM foobar WHERE id = 'a' LIMIT 1")
        self.assertEquals(len(list(results)), 1)

    def test_select_attrs(self):
        """ SELECT statement can fetch only certain attrs """
        self.make_table()
        self.query("INSERT INTO foobar (id, bar, order) VALUES "
                   "('a', 1, 'first'), ('a', 2, 'second')")
        results = self.query("SELECT order FROM foobar "
                             "WHERE id = 'a' and bar = 1")
        results = [dict(r) for r in results]
        self.assertItemsEqual(results, [{'order': 'first'}])

    def test_scan(self):
        """ SCAN statement gets all results in a table """
        self.make_table()
        self.query("INSERT INTO foobar (id, bar) VALUES ('a', 1), ('b', 2)")
        results = self.query("SCAN foobar")
        results = [dict(r) for r in results]
        self.assertItemsEqual(results, [{'id': 'a', 'bar': 1},
                                        {'id': 'b', 'bar': 2}])

    def test_scan_filter(self):
        """ SCAN statement can filter results """
        self.make_table()
        self.query("INSERT INTO foobar (id, bar) VALUES ('a', 1), ('b', 2)")
        results = self.query("SCAN foobar FILTER id = 'a'")
        results = [dict(r) for r in results]
        self.assertItemsEqual(results, [{'id': 'a', 'bar': 1}])

    def test_scan_limit(self):
        """ SCAN statement can filter results """
        self.make_table()
        self.query("INSERT INTO foobar (id, bar) VALUES ('a', 1), ('b', 2)")
        results = self.query("SCAN foobar LIMIT 1")
        self.assertEquals(len(list(results)), 1)

    def test_count(self):
        """ COUNT statement counts items """
        self.make_table()
        self.query("INSERT INTO foobar (id, bar) VALUES ('a', 1), "
                   "('a', 2)")
        count = self.query("COUNT foobar WHERE id = 'a' ")
        self.assertEquals(count, 2)

    def test_delete(self):
        """ DELETE statement removes items """
        table = self.make_table(index='ts')
        self.query("INSERT INTO foobar (id, bar) VALUES ('a', 1), ('b', 2)")
        self.query("DELETE FROM foobar WHERE id = 'a' and bar = 1")
        items = [dict(i) for i in table.scan()]
        self.assertItemsEqual(items, [{'id': 'b', 'bar': 2}])

    def test_delete_in(self):
        """ DELETE Can specify KEYS IN """
        table = self.make_table(index='ts')
        self.query("INSERT INTO foobar (id, bar) VALUES ('a', 1), ('b', 2)")
        self.query("DELETE FROM foobar WHERE KEYS IN ('a', 1)")
        items = [dict(i) for i in table.scan()]
        self.assertItemsEqual(items, [{'id': 'b', 'bar': 2}])

    def test_update(self):
        """ UPDATE sets attributes """
        table = self.make_table()
        self.query("INSERT INTO foobar (id, bar, baz) VALUES ('a', 1, 1), "
                   "('b', 2, 2)")
        self.query("UPDATE foobar SET baz = 3")
        items = [dict(i) for i in table.scan()]
        self.assertItemsEqual(items, [{'id': 'a', 'bar': 1, 'baz': 3},
                                      {'id': 'b', 'bar': 2, 'baz': 3}])

    def test_update_where(self):
        """ UPDATE sets attributes when clause is true """
        table = self.make_table()
        self.query("INSERT INTO foobar (id, bar, baz) VALUES ('a', 1, 1), "
                   "('b', 2, 2)")
        self.query("UPDATE foobar SET baz = 3 WHERE id = 'a'")
        items = [dict(i) for i in table.scan()]
        self.assertItemsEqual(items, [{'id': 'a', 'bar': 1, 'baz': 3},
                                      {'id': 'b', 'bar': 2, 'baz': 2}])

    def test_update_where_in(self):
        """ UPDATE sets attributes for a set of primary keys """
        table = self.make_table()
        self.query("INSERT INTO foobar (id, bar, baz) VALUES ('a', 1, 1), "
                   "('b', 2, 2)")
        self.query("UPDATE foobar SET baz = 3 WHERE KEYS IN ('a', 1), ('b', 2)")
        items = [dict(i) for i in table.scan()]
        self.assertItemsEqual(items, [{'id': 'a', 'bar': 1, 'baz': 3},
                                      {'id': 'b', 'bar': 2, 'baz': 3}])

    def test_update_increment(self):
        """ UPDATE can increment attributes """
        table = self.make_table()
        self.query("INSERT INTO foobar (id, bar, baz) VALUES ('a', 1, 1), "
                   "('b', 2, 2)")
        self.query("UPDATE foobar SET baz += 2")
        self.query("UPDATE foobar SET baz -= 1")
        items = [dict(i) for i in table.scan()]
        self.assertItemsEqual(items, [{'id': 'a', 'bar': 1, 'baz': 2},
                                      {'id': 'b', 'bar': 2, 'baz': 3}])

    def test_update_delete(self):
        """ UPDATE can delete attributes """
        table = self.make_table()
        self.query("INSERT INTO foobar (id, bar, baz) VALUES ('a', 1, 1), "
                   "('b', 2, 2)")
        self.query("UPDATE foobar SET baz = NULL")
        items = [dict(i) for i in table.scan()]
        self.assertItemsEqual(items, [{'id': 'a', 'bar': 1},
                                      {'id': 'b', 'bar': 2}])

    def test_update_returns(self):
        """ UPDATE can specify what the query returns """
        self.make_table()
        self.query("INSERT INTO foobar (id, bar, baz) VALUES ('a', 1, 1), "
                   "('b', 2, 2)")
        result = self.query("UPDATE foobar SET baz = NULL RETURNS ALL NEW ")
        items = [dict(i) for i in result]
        self.assertItemsEqual(items, [{'id': 'a', 'bar': 1},
                                      {'id': 'b', 'bar': 2}])
