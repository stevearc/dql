""" Tests for queries """
from datetime import datetime, timedelta
import time
from dynamo3 import Binary
from dql.models import TableField, IndexField, GlobalIndex

from . import BaseSystemTest

# pylint: disable=W0632


class TestQueries(BaseSystemTest):

    """ System tests for queries """

    def test_drop(self):
        """ DROP statement should drop a table """
        self.query("CREATE TABLE foobar (id STRING HASH KEY)")
        self.query("DROP TABLE foobar")
        table = self.dynamo.describe_table('foobar')
        self.assertIsNone(table)

    def test_drop_if_exists(self):
        """ DROP IF EXISTS shouldn't fail if no table """
        self.query("CREATE TABLE foobar (id STRING HASH KEY)")
        self.query("DROP TABLE foobar")
        self.query("DROP TABLE IF EXISTS foobar")

    def test_explain_drop(self):
        """ EXPLAIN DROP """
        self.query("EXPLAIN DROP TABLE foobar")
        ret = self.engine._call_list
        self.assertEqual(len(ret), 1)
        self.assertEqual(ret[0][0], 'delete_table')
        self.assertEqual(ret[0][1]['TableName'], 'foobar')

    def test_dump(self):
        """ DUMP SCHEMA generates 'create' statements """
        self.query("CREATE TABLE test (id STRING HASH KEY, bar NUMBER RANGE "
                   "KEY, ts NUMBER INDEX('ts-index'), "
                   "baz STRING KEYS INDEX('baz-index'), "
                   "bag NUMBER INCLUDE INDEX('bag-index', ['foo']), "
                   "THROUGHPUT (2, 6)) "
                   "GLOBAL INDEX ('myindex', bar, baz, TP (1, 2)) "
                   "GLOBAL KEYS INDEX ('idx2', id) "
                   "GLOBAL INCLUDE INDEX ('idx3', baz, ['foo', 'foobar'])")
        original = self.engine.describe('test')
        schema = self.query("DUMP SCHEMA")
        self.query("DROP TABLE test")
        self.query(schema)
        new = self.engine.describe('test', True)
        self.assertEquals(original, new)

    def test_dump_tables(self):
        """ DUMP SCHEMA generates 'create' statements for specific tables """
        self.query("CREATE TABLE test (id STRING HASH KEY)")
        self.query("CREATE TABLE test2 (id STRING HASH KEY)")
        schema = self.query("DUMP SCHEMA test2")
        self.query("DROP TABLE test")
        self.query("DROP TABLE test2")
        self.query(schema)
        self.engine.describe('test2', True)
        ret = self.engine.describe('test', True)
        self.assertIsNone(ret)

    def test_multiple_statements(self):
        """ Engine can execute multiple queries separated by ';' """
        result = self.engine.execute("""
            CREATE TABLE test (id STRING HASH KEY);
            INSERT INTO test (id, foo) VALUES ('a', 1), ('b', 2);
            SCAN * FROM test
        """)
        scan_result = list(result)
        self.assertItemsEqual(scan_result, [{'id': 'a', 'foo': 1},
                                            {'id': 'b', 'foo': 2}])


class TestAlter(BaseSystemTest):
    """ Tests for ALTER """

    def test_alter_throughput(self):
        """ Can alter throughput of a table """
        self.query(
            "CREATE TABLE foobar (id STRING HASH KEY, THROUGHPUT (1, 1))")
        self.query("ALTER TABLE foobar SET THROUGHPUT (2, 2)")
        desc = self.engine.describe('foobar', refresh=True)
        self.assertEquals(desc.read_throughput, 2)
        self.assertEquals(desc.write_throughput, 2)

    def test_alter_throughput_partial(self):
        """ Can alter just read or just write throughput of a table """
        self.query(
            "CREATE TABLE foobar (id STRING HASH KEY, THROUGHPUT (1, 1))")
        self.query("ALTER TABLE foobar SET THROUGHPUT (2, 0)")
        desc = self.engine.describe('foobar', refresh=True)
        self.assertEquals(desc.read_throughput, 2)
        self.assertEquals(desc.write_throughput, 1)

    def test_alter_throughput_partial_star(self):
        """ Can alter just read or just write throughput by passing in '*' """
        self.query(
            "CREATE TABLE foobar (id STRING HASH KEY, THROUGHPUT (1, 1))")
        self.query("ALTER TABLE foobar SET THROUGHPUT (2, *)")
        desc = self.engine.describe('foobar', refresh=True)
        self.assertEquals(desc.read_throughput, 2)
        self.assertEquals(desc.write_throughput, 1)

    def test_alter_index_throughput(self):
        """ Can alter throughput of a global index """
        self.query(
            "CREATE TABLE foobar (id STRING HASH KEY, foo NUMBER) "
            "GLOBAL INDEX ('foo_index', foo, THROUGHPUT(1, 1))")
        self.query("ALTER TABLE foobar SET INDEX foo_index THROUGHPUT (2, 2)")
        desc = self.engine.describe('foobar', refresh=True)
        index = desc.global_indexes['foo_index']
        self.assertEquals(index.read_throughput, 2)
        self.assertEquals(index.write_throughput, 2)

    def test_alter_drop(self):
        """ ALTER can drop an index """
        self.query(
            "CREATE TABLE foobar (id STRING HASH KEY, foo NUMBER) "
            "GLOBAL INDEX ('foo_index', foo, THROUGHPUT(1, 1))")
        self.query("ALTER TABLE foobar DROP INDEX foo_index")
        desc = self.engine.describe('foobar', refresh=True)
        if 'foo_index' in desc.global_indexes:
            index = desc.global_indexes['foo_index']
            self.assertEqual(index.status, 'DELETING')
        else:
            self.assertEquals(len(desc.global_indexes.keys()), 0)

    def test_alter_create(self):
        """ ALTER can create an index """
        self.make_table()
        self.query("ALTER TABLE foobar CREATE GLOBAL INDEX ('foo_index', "
                   "baz string, TP (2, 3))")
        desc = self.engine.describe('foobar', refresh=True)
        self.assertTrue('foo_index' in desc.global_indexes)
        index = desc.global_indexes['foo_index']
        self.assertEqual(index.hash_key.name, 'baz')
        self.assertIsNone(index.range_key)
        self.assertEquals(index.read_throughput, 2)
        self.assertEquals(index.write_throughput, 3)

    def test_explain_throughput(self):
        """ EXPLAIN ALTER """
        self.query("EXPLAIN ALTER TABLE foobar SET THROUGHPUT (2, 2)")
        ret = self.engine._call_list
        self.assertEqual(len(ret), 1)
        self.assertEqual(ret[0][0], 'update_table')
        self.assertTrue('ProvisionedThroughput' in ret[0][1])

    def test_explain_create_index(self):
        """ EXPLAIN ALTER create index """
        self.query("EXPLAIN ALTER TABLE foobar CREATE GLOBAL INDEX('foo_index', baz STRING)")
        ret = self.engine._call_list
        self.assertEqual(len(ret), 1)
        self.assertEqual(ret[0][0], 'update_table')
        self.assertTrue('GlobalSecondaryIndexUpdates' in ret[0][1])

    def test_alter_create_if_not_exists(self):
        """ ALTER create index can fail silently """
        self.query(
            "CREATE TABLE foobar (id STRING HASH KEY, foo NUMBER) "
            "GLOBAL INDEX ('foo_index', foo, THROUGHPUT(1, 1))")
        self.query("ALTER TABLE foobar CREATE GLOBAL INDEX "
                   "('foo_index', baz string) IF NOT EXISTS")
        desc = self.engine.describe('foobar', refresh=True)
        self.assertTrue('foo_index' in desc.global_indexes)
        index = desc.global_indexes['foo_index']
        self.assertEqual(index.hash_key.name, 'foo')

    def test_alter_drop_if_exists(self):
        """ ALTER drop index can fail silently """
        self.make_table()
        self.query("ALTER TABLE foobar DROP INDEX foo_index IF EXISTS")


class TestInsert(BaseSystemTest):

    """ Tests for INSERT """
    def test_insert(self):
        """ INSERT should create items """
        table = self.make_table()
        self.query("INSERT INTO foobar (id, bar) VALUES ('a', 1), ('b', 2)")
        items = list(self.dynamo.scan(table))
        self.assertItemsEqual(items, [{'id': 'a', 'bar': 1},
                                      {'id': 'b', 'bar': 2}])

    def test_insert_binary(self):
        """ INSERT can insert binary values """
        self.query("CREATE TABLE foobar (id BINARY HASH KEY)")
        self.query("INSERT INTO foobar (id) VALUES (b'a')")
        items = list(self.dynamo.scan('foobar'))
        self.assertEqual(items, [{'id': Binary(b'a')}])

    def test_insert_keywords(self):
        """ INSERT can specify data in keyword=arg form """
        table = self.make_table(range_key=None)
        self.query("INSERT INTO foobar (id='a', bar=1), (id='b', baz=4)")
        items = list(self.dynamo.scan(table))
        self.assertItemsEqual(items, [{'id': 'a', 'bar': 1},
                                      {'id': 'b', 'baz': 4}])

    def test_insert_timestamps(self):
        """ INSERT can insert timestamps """
        table = self.make_table(range_key=None)
        self.query("INSERT INTO foobar (id='a', bar=NOW() + interval '1 hour')")
        ret = list(self.dynamo.scan(table))[0]
        now = time.time()
        self.assertTrue(abs(int(now + 60 * 60) - int(ret['bar'])) <= 1)

    def test_explain(self):
        """ EXPLAIN INSERT """
        self.query("EXPLAIN INSERT INTO foobar (id) VALUES ('a')")
        ret = self.engine._call_list
        self.assertEqual(len(ret), 1)
        self.assertEqual(ret[0][0], 'batch_write_item')


class TestSelect(BaseSystemTest):

    """ Tests for SELECT """

    def test_hash_key(self):
        """ SELECT filters by hash key """
        self.make_table()
        self.query("INSERT INTO foobar (id, bar) VALUES ('a', 1), ('b', 2)")
        results = self.query("SELECT * FROM foobar WHERE id = 'a'")
        results = list(results)
        self.assertItemsEqual(results, [{'id': 'a', 'bar': 1}])

    def test_consistent(self):
        """ SELECT can force consistent read """
        self.make_table(range_key=None)
        self.query("INSERT INTO foobar (id='a')")
        results = self.query("SELECT CONSISTENT * FROM foobar WHERE id = 'a'")
        results = list(results)
        self.assertItemsEqual(results, [{'id': 'a'}])

    def test_hash_range(self):
        """ SELECT filters by hash and range keys """
        self.make_table()
        self.query("INSERT INTO foobar (id, bar) VALUES ('a', 1), ('b', 2)")
        results = self.query("SELECT * FROM foobar WHERE id = 'a' and bar = 1")
        results = list(results)
        self.assertItemsEqual(results, [{'id': 'a', 'bar': 1}])

    def test_get(self):
        """ SELECT statement can fetch items directly """
        self.make_table()
        self.query("INSERT INTO foobar (id, bar) VALUES ('a', 1), ('b', 2)")
        results = self.query("SELECT * FROM foobar KEYS IN "
                             "('a', 1), ('b', 2)")
        results = list(results)
        self.assertItemsEqual(results, [{'id': 'a', 'bar': 1},
                                        {'id': 'b', 'bar': 2}])

    def test_reverse(self):
        """ SELECT can reverse order of results """
        self.make_table()
        self.query("INSERT INTO foobar (id, bar) VALUES ('a', 1), ('a', 2)")
        results = self.query("SELECT * FROM foobar WHERE id = 'a' ASC")
        rev_results = self.query("SELECT * FROM foobar WHERE id = 'a' DESC")
        results = list(results)
        rev_results = list(reversed(list(rev_results)))
        self.assertEquals(results, rev_results)

    def test_hash_index(self):
        """ SELECT filters by indexes """
        self.make_table(index='ts')
        self.query("INSERT INTO foobar (id, bar, ts) VALUES ('a', 1, 100), "
                   "('a', 2, 200)")
        results = self.query("SELECT * FROM foobar WHERE id = 'a' "
                             "and ts < 150 USING ts-index")
        results = list(results)
        self.assertItemsEqual(results, [{'id': 'a', 'bar': 1, 'ts': 100}])

    def test_smart_index(self):
        """ SELECT auto-selects correct index name """
        self.make_table(index='ts')
        self.query("INSERT INTO foobar (id, bar, ts) VALUES ('a', 1, 100), "
                   "('a', 2, 200)")
        results = self.query("SELECT * FROM foobar WHERE id = 'a' "
                             "and ts < 150")
        results = list(results)
        self.assertItemsEqual(results, [{'id': 'a', 'bar': 1, 'ts': 100}])

    def test_smart_global_index(self):
        """ SELECT auto-selects correct global index name """
        self.query("CREATE TABLE foobar (id STRING HASH KEY, foo STRING "
                   "RANGE KEY, bar NUMBER INDEX('bar-index'), baz STRING) "
                   "GLOBAL INDEX ('gindex', baz)")
        self.query("INSERT INTO foobar (id, foo, bar, baz) VALUES "
                   "('a', 'a', 1, 'a'), ('b', 'b', 2, 'b')")
        results = list(self.query("SELECT * FROM foobar WHERE baz = 'a'"))
        self.assertItemsEqual(results, [{'id': 'a', 'foo': 'a',
                                         'bar': 1, 'baz': 'a'}])

    def test_limit(self):
        """ SELECT can specify limit """
        self.make_table()
        self.query("INSERT INTO foobar (id, bar, ts) VALUES ('a', 1, 100), "
                   "('a', 2, 200)")
        results = self.query("SELECT * FROM foobar WHERE id = 'a' LIMIT 1")
        self.assertEquals(len(list(results)), 1)

    def test_scan_item_limit(self):
        """ SELECT can provide a LIMIT and SCAN LIMIT """
        self.make_table()
        self.query("INSERT INTO foobar (id, bar, ts) VALUES ('a', 1, 100), "
                   "('a', 2, 200), ('a', 3, 300)")
        results = self.query("SELECT * FROM foobar WHERE id = 'a' and "
                             "ts > 200 LIMIT 1 SCAN LIMIT 2")
        self.assertEquals(len(list(results)), 0)

    def test_attrs(self):
        """ SELECT can fetch only certain attrs """
        self.make_table()
        self.query("INSERT INTO foobar (id, bar, order) VALUES "
                   "('a', 1, 'first'), ('a', 2, 'second')")
        results = self.query("SELECT order FROM foobar "
                             "WHERE id = 'a' and bar = 1")
        results = [dict(r) for r in results]
        self.assertItemsEqual(results, [{'order': 'first'}])

    def test_begins_with(self):
        """ SELECT can filter attrs that begin with a string """
        self.query("CREATE TABLE foobar (id NUMBER HASH KEY, "
                   "bar STRING RANGE KEY)")
        self.query("INSERT INTO foobar (id, bar) VALUES "
                   "(1, 'abc'), (1, 'def')")
        results = self.query("SELECT * FROM foobar "
                             "WHERE id = 1 AND begins_with(bar, 'a')")
        results = [dict(r) for r in results]
        self.assertItemsEqual(results, [{'id': 1, 'bar': 'abc'}])

    def test_between(self):
        """ SELECT can filter attrs that are between values"""
        self.make_table()
        self.query("INSERT INTO foobar (id, bar) VALUES "
                   "('a', 5), ('a', 10)")
        results = self.query("SELECT * FROM foobar "
                             "WHERE id = 'a' AND bar BETWEEN 1 AND 8")
        results = [dict(r) for r in results]
        self.assertItemsEqual(results, [{'id': 'a', 'bar': 5}])

    def test_filter(self):
        """ SELECT can filter results before returning them """
        self.make_table()
        self.query("INSERT INTO foobar (id, bar, baz) VALUES "
                   "('a', 1, 1), ('a', 2, 2)")
        results = self.query("SELECT * FROM foobar "
                             "WHERE id = 'a' AND baz = 1")
        results = [dict(r) for r in results]
        self.assertItemsEqual(results, [{'id': 'a', 'bar': 1, 'baz': 1}])

    def test_filter_and(self):
        """ SELECT can use multi-conditional filter on results """
        self.make_table()
        self.query("INSERT INTO foobar (id, foo, bar, baz) VALUES "
                   "('a', 1, 1, 1), ('a', 2, 2, 1)")
        results = self.query("SELECT * FROM foobar "
                             "WHERE id = 'a' AND baz = 1 AND foo = 1")
        results = [dict(r) for r in results]
        self.assertItemsEqual(results, [{'id': 'a', 'foo': 1, 'bar': 1, 'baz': 1}])

    def test_filter_or(self):
        """ SELECT can use multi-conditional OR filter on results """
        self.make_table()
        self.query("INSERT INTO foobar (id, foo, bar, baz) VALUES "
                   "('a', 1, 1, 1), ('a', 2, 2, 2)")
        results = self.query("SELECT * FROM foobar "
                             "WHERE id = 'a' AND (baz = 1 OR foo = 2)")
        results = [dict(r) for r in results]
        self.assertItemsEqual(results, [{'id': 'a', 'foo': 1, 'bar': 1, 'baz': 1},
                                        {'id': 'a', 'foo': 2, 'bar': 2, 'baz': 2}])

    def test_count(self):
        """ SELECT can items """
        self.make_table()
        self.query("INSERT INTO foobar (id, bar) VALUES ('a', 1), "
                   "('a', 2)")
        count = self.query("SELECT count(*) FROM foobar WHERE id = 'a'")
        self.assertEquals(count, 2)
        self.assertEquals(count.scanned_count, 2)

    def test_count_smart_index(self):
        """ SELECT count(*) auto-selects correct index name """
        self.make_table(index='ts')
        self.query("INSERT INTO foobar (id, bar, ts) VALUES ('a', 1, 100), "
                   "('a', 2, 200)")
        count = self.query("SELECT count(*) FROM foobar "
                           "WHERE id = 'a' and ts < 150")
        self.assertEquals(count, 1)
        self.assertEquals(count.scanned_count, 1)

    def test_count_filter(self):
        """ SELECT count(*) can use conditional filter on results """
        self.make_table()
        self.query("INSERT INTO foobar (id, foo, bar) VALUES "
                   "('a', 1, 1), ('a', 2, 2)")
        count = self.query("SELECT count(*) FROM foobar "
                           "WHERE id = 'a' AND foo = 1")
        self.assertEqual(count, 1)
        self.assertEqual(count.scanned_count, 2)

    def test_explain_select(self):
        """ EXPLAIN SELECT """
        self.make_table(range_key=None)
        self.query("EXPLAIN SELECT * FROM foobar WHERE id = 'a'")
        ret = self.engine._call_list
        self.assertEqual(len(ret), 1)
        self.assertEqual(ret[0][0], 'query')

    def test_explain_select_keys_in(self):
        """ EXPLAIN SELECT KEYS IN"""
        self.make_table(range_key=None)
        self.query("EXPLAIN SELECT * FROM foobar KEYS IN 'a', 'b'")
        ret = self.engine._call_list
        self.assertEqual(len(ret), 1)
        self.assertEqual(ret[0][0], 'batch_get_item')

    def test_order_by_index(self):
        """ SELECT data ORDER BY range key """
        self.make_table()
        self.query("INSERT INTO foobar (id, bar) VALUES "
                   "('a', 1), ('a', 3), ('a', 2)")
        ret = self.query("SELECT * FROM foobar WHERE id = 'a' ORDER BY bar")
        ret = list(ret)
        expected = [
            {'id': 'a', 'bar': 1},
            {'id': 'a', 'bar': 2},
            {'id': 'a', 'bar': 3}
        ]
        self.assertEqual(ret, expected)
        ret = self.query("SELECT * FROM foobar WHERE id = 'a' ORDER BY bar DESC")
        ret = list(ret)
        expected.reverse()
        self.assertEqual(ret, expected)

    def test_order_by(self):
        """ SELECT data ORDER BY non-range key """
        self.make_table()
        self.query("INSERT INTO foobar (id, bar, baz) VALUES "
                   "('a', 1, 20), ('a', 2, 30), ('a', 3, 10)")
        ret = self.query("SELECT * FROM foobar WHERE id = 'a' ORDER BY baz")
        expected = [
            {'id': 'a', 'bar': 3, 'baz': 10},
            {'id': 'a', 'bar': 1, 'baz': 20},
            {'id': 'a', 'bar': 2, 'baz': 30},
        ]
        self.assertEqual(list(ret), expected)
        ret = self.query("SELECT * FROM foobar WHERE id = 'a' ORDER BY baz DESC")
        expected.reverse()
        self.assertEqual(list(ret), expected)

    def test_select_non_projected(self):
        """ SELECT can get attributes not projected onto an index """
        self.query("CREATE TABLE foobar (id STRING HASH KEY, foo STRING) "
                   "GLOBAL KEYS INDEX ('gindex', foo)")
        self.query("INSERT INTO foobar (id, foo, bar) VALUES "
                   "('a', 'a', 1), ('b', 'b', 2)")
        ret = self.query("SELECT bar FROM foobar WHERE foo = 'b' USING gindex")
        self.assertEqual(list(ret), [{'bar': 2}])


class TestSelectScan(BaseSystemTest):

    """ Tests for SELECT that involve doing a table scan """
    def setUp(self):
        super(TestSelectScan, self).setUp()
        self.engine.allow_select_scan = True

    def _run(self, query, expected):
        """ Test the query both with SELECT and SCAN """
        for cmd in ['SELECT', 'SCAN']:
            results = self.query(cmd + ' ' + query)
            results = list(results)
            if isinstance(expected, int):
                self.assertEqual(len(results), expected)
            else:
                self.assertItemsEqual(results, expected)

    def test_scan(self):
        """ SELECT scan gets all results in a table """
        self.make_table()
        self.query("INSERT INTO foobar (id, bar) VALUES ('a', 1), ('b', 2)")
        self._run("* FROM foobar",
                  [{'id': 'a', 'bar': 1}, {'id': 'b', 'bar': 2}])

    def test_filter(self):
        """ SELECT scan can filter results """
        self.make_table()
        self.query("INSERT INTO foobar (id, bar) VALUES ('a', 1), ('b', 2)")
        self._run("* FROM foobar WHERE bar = 2",
                  [{'id': 'b', 'bar': 2}])

    def test_limit(self):
        """ SELECT scan can limit results """
        self.make_table()
        self.query("INSERT INTO foobar (id, bar) VALUES ('a', 1), ('b', 2)")
        self._run("* FROM foobar LIMIT 1", 1)

    def test_scan_limit(self):
        """ SELECT scan can limit the number of items scanned """
        self.make_table()
        self.query("INSERT INTO foobar (id, bar) VALUES ('a', 1), ('b', 2)")
        self._run("* FROM foobar SCAN LIMIT 1", 1)

    def test_begins_with(self):
        """ SELECT scan can filter attrs that begin with a string """
        self.query("CREATE TABLE foobar (id NUMBER HASH KEY, "
                   "bar STRING RANGE KEY)")
        self.query("INSERT INTO foobar (id, bar) VALUES "
                   "(1, 'abc'), (1, 'def')")
        self._run("* FROM foobar WHERE begins_with(bar, 'a')",
                  [{'id': 1, 'bar': 'abc'}])

    def test_between(self):
        """ SELECT scan can filter attrs that are between values"""
        self.make_table()
        self.query("INSERT INTO foobar (id, bar) VALUES "
                   "('a', 5), ('a', 10)")
        self._run("* FROM foobar WHERE bar BETWEEN 1 AND 8",
                  [{'id': 'a', 'bar': 5}])

    def test_null(self):
        """ SELECT scan can filter if an attr is null """
        self.make_table()
        self.query("INSERT INTO foobar (id, bar) VALUES ('a', 5)")
        self.query("INSERT INTO foobar (id, bar, baz) VALUES ('a', 1, 1)")
        self._run("* FROM foobar WHERE attribute_not_exists(baz)",
                  [{'id': 'a', 'bar': 5}])

    def test_not_null(self):
        """ SELECT scan can filter if an attr is not null """
        self.make_table()
        self.query("INSERT INTO foobar (id, bar) VALUES ('a', 5)")
        self.query("INSERT INTO foobar (id, bar, baz) VALUES ('a', 1, 1)")
        self._run("* FROM foobar WHERE attribute_exists(baz)",
                  [{'id': 'a', 'bar': 1, 'baz': 1}])

    def test_in(self):
        """ SELECT scan can filter if an attr is in a set """
        self.make_table()
        self.query("INSERT INTO foobar (id, bar) VALUES ('a', 5), ('a', 2)")
        self._run("* FROM foobar WHERE bar IN (1, 3, 5)",
                  [{'id': 'a', 'bar': 5}])

    def test_contains(self):
        """ SELECT scan can filter if a set contains an item """
        self.make_table()
        self.query("INSERT INTO foobar (id, bar, baz) VALUES "
                   "('a', 5, (1, 2, 3)), ('a', 1, (4, 5, 6))")
        self._run("* FROM foobar WHERE contains(baz, 2)",
                  [{'id': 'a', 'bar': 5, 'baz': set([1, 2, 3])}])

    def test_filter_and(self):
        """ SELECT scan can use multi-conditional filter """
        self.make_table()
        self.query("INSERT INTO foobar (id, foo, bar) VALUES ('a', 1, 1), ('b', 1, 2)")
        self._run("* FROM foobar WHERE foo = 1 AND bar = 1",
                  [{'id': 'a', 'foo': 1, 'bar': 1}])

    def test_filter_or(self):
        """ SELECT scan can use multi-conditional OR filter """
        self.make_table()
        self.query("INSERT INTO foobar (id, foo, bar) VALUES "
                   "('a', 1, 1), ('b', 2, 2)")
        self._run("* FROM foobar WHERE foo = 1 OR bar = 2",
                  [{'id': 'a', 'foo': 1, 'bar': 1},
                   {'id': 'b', 'foo': 2, 'bar': 2}])

    def test_filter_nested(self):
        """ SELECT scan can use nested conditional filters """
        self.make_table()
        self.query("INSERT INTO foobar (id, foo, bar) VALUES "
                   "('a', 1, 1), ('b', 1, 2), ('c', 1, 3)")
        self._run("* FROM foobar WHERE foo = 1 AND NOT (bar = 2 OR bar = 3)",
                  [{'id': 'a', 'foo': 1, 'bar': 1}])

    def test_scan_global(self):
        """ SELECT scan can scan a global index """
        self.query("CREATE TABLE foobar (id STRING HASH KEY, foo STRING) "
                   "GLOBAL KEYS INDEX ('gindex', foo)")
        self.query("INSERT INTO foobar (id, foo) VALUES "
                   "('a', 'a')")
        self._run("* FROM foobar USING gindex", [{'id': 'a', 'foo': 'a'}])

    def test_scan_global_with_constraints(self):
        """ SELECT scan can scan a global index and filter """
        self.query("CREATE TABLE foobar (id STRING HASH KEY, foo STRING) "
                   "GLOBAL KEYS INDEX ('gindex', foo)")
        self.query("INSERT INTO foobar (id, foo) VALUES "
                   "('a', 'a'), ('b', 'b')")
        self._run("* FROM foobar WHERE id = 'a' USING gindex",
                  [{'id': 'a', 'foo': 'a'}])

    def test_filter_list(self):
        """ SELECT scan can filter based on elements in a list """
        self.make_table(range_key=None)
        self.query("INSERT INTO foobar (id, bar) VALUES ('a', [1, 2]), ('b', [2, 3])")
        self.engine.reserved_words = None
        self._run("* FROM foobar WHERE bar[0] = 2",
                  [{'id': 'b', 'bar': [2, 3]}])

    def test_filter_map(self):
        """ SELECT scan can filter based on values in a map """
        self.make_table(range_key=None)
        self.query("INSERT INTO foobar (id, bar) VALUES ('a', {'b': 1}), ('b', {'b': 2})")
        self.engine.reserved_words = None
        self._run("* FROM foobar WHERE bar.b = 2",
                  [{'id': 'b', 'bar': {'b': 2}}])

    def test_explain_scan(self):
        """ EXPLAIN SELECT """
        self.make_table(range_key=None)
        self.query("EXPLAIN SELECT * FROM foobar WHERE bar = 'a'")
        ret = self.engine._call_list
        self.assertEqual(len(ret), 1)
        self.assertEqual(ret[0][0], 'scan')

    def test_field_ne_field(self):
        """ SELECT can filter fields compared to other fields """
        self.make_table(range_key=None)
        self.query("INSERT INTO foobar (id, bar, baz) VALUES ('a', 1, 1), ('b', 2, 3)")
        self._run("* FROM foobar WHERE bar != baz",
                  [{'id': 'b', 'bar': 2, 'baz': 3}])

    def test_select_filter_timestamp(self):
        """ SELECT can filter by timestamp """
        self.make_table(range_key=None)
        self.query("INSERT INTO foobar (id='a', bar=now() + interval '1 hour')")
        ret = list(self.query("SCAN * FROM foobar WHERE bar > NOW()"))
        self.assertEqual(len(ret), 1)

    def test_select_alias(self):
        """ SELECT can alias selected fields """
        self.make_table(range_key=None)
        self.query("INSERT INTO foobar (id='a', bar=5)")
        self._run("id, bar as baz FROM foobar",
                  [{'id': 'a', 'baz': 5}])

    def test_select_operation(self):
        """ SELECT can perform simple arithmetic """
        self.make_table(range_key=None)
        self.query("INSERT INTO foobar (id='a', bar=5, baz=3)")
        self._run("bar + baz as ret FROM foobar",
                  [{'ret': 8}])

    def test_select_none_operation(self):
        """ SELECT operations ignore None values """
        self.make_table(range_key=None)
        self.query("INSERT INTO foobar (id='a', bar=5)")
        self._run("bar + baz as ret FROM foobar",
                  [{'ret': 5}])

    def test_select_type_error_operation(self):
        """ SELECT operations with bad values return TypeError """
        self.make_table(range_key=None)
        self.query("INSERT INTO foobar (id='a', bar=5, baz=(1, 2))")
        ret = list(self.query("SCAN bar + baz as ret FROM foobar"))[0]
        self.assertTrue(isinstance(ret['ret'], TypeError))

    def test_nested_operation(self):
        """ SELECT can perform nested operations """
        self.make_table(range_key=None)
        self.query("INSERT INTO foobar (id='a', foo=10, bar=5, baz=3)")
        self._run("foo - (bar - baz) as ret FROM foobar",
                  [{'ret': 8}])

    def test_select_timestamp(self):
        """ SELECT can convert values to timestamps """
        self.make_table(range_key=None)
        self.query("INSERT INTO foobar (id='a', bar=NOW())")
        ret = list(self.query("SCAN ts(bar) as bar FROM foobar"))[0]
        self.assertTrue(isinstance(ret['bar'], datetime))

    def test_select_timestamp_ms(self):
        """ SELECT can convert millisecond values to timestamps """
        self.make_table(range_key=None)
        self.query("INSERT INTO foobar (id='a', bar=ms(NOW()))")
        ret = list(self.query("SCAN ts(bar) as bar FROM foobar"))[0]
        self.assertTrue(isinstance(ret['bar'], datetime))

    def test_select_timestamp_literal(self):
        """ SELECT can parse timestamp literals """
        self.make_table(range_key=None)
        self.query("INSERT INTO foobar (id='a', bar=4)")
        ret = list(self.query("SCAN ts('2015-12-5') as d FROM foobar"))[0]
        self.assertTrue(isinstance(ret['d'], datetime))

    def test_select_now(self):
        """ SELECT can get the current time """
        self.make_table(range_key=None)
        self.query("INSERT INTO foobar (id='a', bar=4)")
        ret = list(self.query("SCAN now() as d FROM foobar"))[0]
        self.assertTrue(isinstance(ret['d'], datetime))

    def test_select_timedelta(self):
        """ SELECT can subtract dates to get a timedelta """
        self.make_table(range_key=None)
        self.query("INSERT INTO foobar (id='a', bar=now())")
        ret = list(self.query("SCAN now() - ts(bar) as d FROM foobar"))[0]
        self.assertTrue(isinstance(ret['d'], timedelta))


class TestCreate(BaseSystemTest):

    """ Tests for CREATE """

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
        self.assertEqual(desc.attrs, {
            'owner': TableField('owner', 'STRING', 'HASH'),
            'id': TableField('id', 'BINARY', 'RANGE'),
            'ts': IndexField('ts', 'NUMBER', 'ALL', 'ts-index'),
        })

    def test_create_throughput(self):
        """ CREATE statement can specify throughput """
        self.query(
            "CREATE TABLE foobar (id STRING HASH KEY, THROUGHPUT (1, 2))")
        desc = self.engine.describe('foobar')
        self.assertEquals(desc.read_throughput, 1)
        self.assertEquals(desc.write_throughput, 2)

    def test_create_if_not_exists(self):
        """ CREATE IF NOT EXISTS shouldn't fail if table exists """
        self.query("CREATE TABLE foobar (owner STRING HASH KEY)")
        self.query("CREATE TABLE IF NOT EXISTS foobar (owner STRING HASH KEY)")

    def test_create_keys_index(self):
        """ Can create a keys-only index """
        self.query(
            """
            CREATE TABLE foobar (owner STRING HASH KEY,
                                 id BINARY RANGE KEY,
                                 ts NUMBER KEYS INDEX('ts-index'))
            """)
        desc = self.engine.describe('foobar')
        self.assertEqual(desc.attrs['ts'], IndexField('ts', 'NUMBER',
                                                      'KEYS', 'ts-index'))

    def test_create_include_index(self):
        """ Can create an include-only index """
        self.query(
            """
            CREATE TABLE foobar (owner STRING HASH KEY,
                id BINARY RANGE KEY,
                ts NUMBER INCLUDE INDEX('ts-index', ['foo', 'bar']))
            """)
        desc = self.engine.describe('foobar')
        self.assertEqual(desc.attrs['ts'], IndexField('ts', 'NUMBER',
                                                      'INCLUDE', 'ts-index',
                                                      ['foo', 'bar']))

    def test_create_global_indexes(self):
        """ Can create with global indexes """
        self.query(
            "CREATE TABLE foobar (id STRING HASH KEY, foo NUMBER RANGE KEY) "
            "GLOBAL INDEX ('myindex', foo, id, THROUGHPUT (1, 2))"
        )
        desc = self.engine.describe('foobar')
        hash_key = TableField('foo', 'NUMBER', 'HASH')
        range_key = TableField('id', 'STRING', 'RANGE')
        gindex = GlobalIndex('myindex', 'ALL', 'ACTIVE', hash_key, range_key, 1, 2, 0)
        self.assertEquals(desc.global_indexes, {
            'myindex': gindex,
        })

    def test_create_global_index_types(self):
        """ Global indexes can specify the attribute types """
        self.query(
            "CREATE TABLE foobar (id STRING HASH KEY, foo NUMBER RANGE KEY) "
            "GLOBAL INDEX ('myindex', foo number, baz string, THROUGHPUT (1, 2))"
        )
        desc = self.engine.describe('foobar')
        hash_key = TableField('foo', 'NUMBER', 'HASH')
        range_key = TableField('baz', 'STRING', 'RANGE')
        gindex = GlobalIndex('myindex', 'ALL', 'ACTIVE', hash_key, range_key, 1, 2, 0)
        self.assertEquals(desc.global_indexes, {
            'myindex': gindex,
        })

    def test_create_global_index_no_range(self):
        """ Can create global index with no range key """
        self.query(
            "CREATE TABLE foobar (id STRING HASH KEY, foo NUMBER) "
            "GLOBAL ALL INDEX ('myindex', foo, THROUGHPUT (1, 2))"
        )
        desc = self.engine.describe('foobar')
        hash_key = TableField('foo', 'NUMBER', 'HASH')
        gindex = GlobalIndex('myindex', 'ALL', 'ACTIVE', hash_key, None, 1, 2, 0)
        self.assertEquals(desc.global_indexes, {
            'myindex': gindex,
        })

    def test_create_global_keys_index(self):
        """ Can create a global keys-only index """
        self.query(
            "CREATE TABLE foobar (id STRING HASH KEY, foo NUMBER) "
            "GLOBAL KEYS INDEX ('myindex', foo, THROUGHPUT (1, 2))"
        )
        desc = self.engine.describe('foobar')
        hash_key = TableField('foo', 'NUMBER', 'HASH')
        gindex = GlobalIndex('myindex', 'KEYS', 'ACTIVE', hash_key, None, 1, 2, 0)
        self.assertEquals(desc.global_indexes, {
            'myindex': gindex,
        })

    def test_create_global_include_index(self):
        """ Can create a global include-only index """
        self.query(
            "CREATE TABLE foobar (id STRING HASH KEY, foo NUMBER) "
            "GLOBAL INCLUDE INDEX ('myindex', foo, ['bar', 'baz'], THROUGHPUT (1, 2))"
        )
        desc = self.engine.describe('foobar')
        hash_key = TableField('foo', 'NUMBER', 'HASH')
        gindex = GlobalIndex('myindex', 'INCLUDE', 'ACTIVE', hash_key, None, 1, 2, 0, ['bar', 'baz'])
        self.assertEquals(desc.global_indexes, {
            'myindex': gindex,
        })

    def test_create_explain(self):
        """ EXPLAIN CREATE """
        self.query("EXPLAIN CREATE TABLE foobar (id STRING HASH KEY)")
        ret = self.engine._call_list
        self.assertEqual(len(ret), 1)
        self.assertEqual(ret[0][0], 'create_table')


class TestUpdate(BaseSystemTest):

    """ Tests for UPDATE """

    def test_update(self):
        """ UPDATE sets attributes """
        table = self.make_table()
        self.query("INSERT INTO foobar (id, bar, baz) VALUES ('a', 1, 1), "
                   "('b', 2, 2)")
        self.query("UPDATE foobar SET baz = 3")
        items = list(self.dynamo.scan(table))
        self.assertItemsEqual(items, [{'id': 'a', 'bar': 1, 'baz': 3},
                                      {'id': 'b', 'bar': 2, 'baz': 3}])

    def test_update_where(self):
        """ UPDATE sets attributes when clause is true """
        table = self.make_table()
        self.query("INSERT INTO foobar (id, bar, baz) VALUES ('a', 1, 1), "
                   "('b', 2, 2)")
        self.query("UPDATE foobar SET baz = 3 WHERE id = 'a'")
        items = list(self.dynamo.scan(table))
        self.assertItemsEqual(items, [{'id': 'a', 'bar': 1, 'baz': 3},
                                      {'id': 'b', 'bar': 2, 'baz': 2}])

    def test_update_count(self):
        """ UPDATE returns number of records updated """
        self.make_table()
        self.query("INSERT INTO foobar (id, bar, baz) VALUES ('a', 1, 1), "
                   "('b', 2, 2)")
        count = self.query("UPDATE foobar SET baz = 3 WHERE id = 'a'")
        self.assertEqual(count, 1)

    def test_update_where_in(self):
        """ UPDATE can update items by their primary keys """
        table = self.make_table()
        self.query("INSERT INTO foobar (id, bar, baz) VALUES ('a', 1, 1), "
                   "('b', 2, 2)")
        self.query("UPDATE foobar SET baz = 3 KEYS IN ('a', 1), ('b', 2)")
        items = list(self.dynamo.scan(table))
        self.assertItemsEqual(items, [{'id': 'a', 'bar': 1, 'baz': 3},
                                      {'id': 'b', 'bar': 2, 'baz': 3}])

    def test_update_in_condition(self):
        """ UPDATE can alert items using KEYS IN and WHERE """
        table = self.make_table(range_key=None)
        self.query("INSERT INTO foobar (id, bar) VALUES ('a', 1), ('b', 2)")
        self.query("UPDATE foobar SET bar = 3 KEYS IN ('a', 1), ('b', 2) "
                   "WHERE bar < 2")
        items = list(self.dynamo.scan(table))
        self.assertItemsEqual(items, [{'id': 'a', 'bar': 3},
                                      {'id': 'b', 'bar': 2}])

    def test_update_keys_count(self):
        """ UPDATE returns number of records updated with KEYS IN """
        self.make_table()
        self.query("INSERT INTO foobar (id, bar, baz) VALUES ('a', 1, 1), "
                   "('b', 2, 2)")
        ret = self.query("UPDATE foobar SET baz = 3 KEYS IN ('a', 1), "
                         "('b', 2)")
        self.assertEqual(ret, 2)

    def test_update_increment(self):
        """ UPDATE can increment attributes """
        table = self.make_table()
        self.query("INSERT INTO foobar (id, bar, baz) VALUES ('a', 1, 1), "
                   "('b', 2, 2)")
        self.query("UPDATE foobar ADD baz 2")
        self.query("UPDATE foobar ADD baz -1")
        items = list(self.dynamo.scan(table))
        self.assertItemsEqual(items, [{'id': 'a', 'bar': 1, 'baz': 2},
                                      {'id': 'b', 'bar': 2, 'baz': 3}])

    def test_update_add(self):
        """ UPDATE can add elements to set """
        table = self.make_table()
        self.query("INSERT INTO foobar (id, bar, baz) VALUES ('a', 1, ())")
        self.query("UPDATE foobar ADD baz (1)")
        self.query("UPDATE foobar ADD baz (2, 3)")
        items = list(self.dynamo.scan(table))
        self.assertItemsEqual(items, [{'id': 'a', 'bar': 1,
                                       'baz': set([1, 2, 3])}])

    def test_update_delete(self):
        """ UPDATE can delete elements from set """
        table = self.make_table()
        self.query("INSERT INTO foobar (id, bar, baz) VALUES "
                   "('a', 1, (1, 2, 3, 4))")
        self.query("UPDATE foobar DELETE baz (2)")
        self.query("UPDATE foobar DELETE baz (1, 3)")
        items = list(self.dynamo.scan(table))
        self.assertItemsEqual(items, [{'id': 'a', 'bar': 1, 'baz': set([4])}])

    def test_update_remove(self):
        """ UPDATE can remove attributes """
        table = self.make_table()
        self.query("INSERT INTO foobar (id, bar, baz) VALUES ('a', 1, 1), "
                   "('b', 2, 2)")
        self.query("UPDATE foobar REMOVE baz")
        items = list(self.dynamo.scan(table))
        self.assertItemsEqual(items, [{'id': 'a', 'bar': 1},
                                      {'id': 'b', 'bar': 2}])

    def test_update_returns(self):
        """ UPDATE can specify what the query returns """
        self.make_table()
        self.query("INSERT INTO foobar (id, bar, baz) VALUES ('a', 1, 1), "
                   "('b', 2, 2)")
        result = self.query("UPDATE foobar REMOVE baz RETURNS ALL NEW ")
        items = list(result)
        self.assertItemsEqual(items, [{'id': 'a', 'bar': 1},
                                      {'id': 'b', 'bar': 2}])

    def test_update_soft(self):
        """ UPDATE can set a field if it doesn't exist """
        table = self.make_table(range_key=None)
        self.query("INSERT INTO foobar (id, bar) VALUES ('a', 1), ('b', NULL)")
        self.query("UPDATE foobar SET bar = if_not_exists(bar, 2)")
        items = list(self.dynamo.scan(table))
        self.assertItemsEqual(items, [{'id': 'a', 'bar': 1},
                                      {'id': 'b', 'bar': 2}])

    def test_update_append(self):
        """ UPDATE can append to a list """
        table = self.make_table(range_key=None)
        self.query("INSERT INTO foobar (id, bar) VALUES ('a', [1])")
        self.query("UPDATE foobar SET bar = list_append(bar, [2])")
        items = list(self.dynamo.scan(table))
        self.assertItemsEqual(items, [{'id': 'a', 'bar': [1, 2]}])

    def test_update_prepend(self):
        """ UPDATE can prepend to a list """
        table = self.make_table(range_key=None)
        self.query("INSERT INTO foobar (id, bar) VALUES ('a', [1])")
        self.query("UPDATE foobar SET bar = list_append([2], bar)")
        items = list(self.dynamo.scan(table))
        self.assertItemsEqual(items, [{'id': 'a', 'bar': [2, 1]}])

    def test_update_condition(self):
        """ UPDATE can conditionally update """
        table = self.make_table(range_key=None)
        self.query("INSERT INTO foobar (id, bar) VALUES ('a', 1), ('b', 2)")
        self.query("UPDATE foobar SET bar = 3 WHERE bar < 2")
        items = list(self.dynamo.scan(table))
        self.assertItemsEqual(items, [{'id': 'a', 'bar': 3},
                                      {'id': 'b', 'bar': 2}])

    def test_update_index(self):
        """ UPDATE can query an index for the items to update """
        table = self.make_table(index='ts')
        self.query("INSERT INTO foobar (id, bar, ts) VALUES ('a', 1, 100)")
        self.query("UPDATE foobar SET ts = 3 WHERE id = 'a' USING ts-index")
        items = list(self.dynamo.scan(table))
        self.assertItemsEqual(items, [{'id': 'a', 'bar': 1, 'ts': 3}])

    def test_explain_update(self):
        """ EXPLAIN UPDATE """
        self.make_table()
        self.query("EXPLAIN UPDATE foobar SET baz = 1 WHERE id = 'a'")
        ret = self.engine._call_list
        self.assertEqual(len(ret), 2)
        query, update = ret
        self.assertEqual(query[0], 'query')
        self.assertEqual(update[0], 'update_item')

    def test_explain_update_get(self):
        """ EXPLAIN UPDATE batch get item """
        self.make_table(range_key=None)
        self.query("EXPLAIN UPDATE foobar SET baz = 1 KEYS IN 'a', 'b'")
        ret = self.engine._call_list
        self.assertEqual(len(ret), 1)
        self.assertEqual(ret[0][0], 'update_item')

    def test_explain_update_scan(self):
        """ EXPLAIN UPDATE scan """
        self.make_table(range_key=None)
        self.query("EXPLAIN UPDATE foobar SET baz = 1 where bar='a'")
        ret = self.engine._call_list
        self.assertEqual(len(ret), 2)
        scan, update = ret
        self.assertEqual(scan[0], 'scan')
        self.assertEqual(update[0], 'update_item')


class TestDelete(BaseSystemTest):

    """ Tests for DELETE """

    def test_delete(self):
        """ DELETE removes items """
        table = self.make_table(index='ts')
        self.query("INSERT INTO foobar (id, bar) VALUES ('a', 1), ('b', 2)")
        self.query("DELETE FROM foobar")
        items = list(self.dynamo.scan(table))
        self.assertEqual(len(items), 0)

    def test_delete_where(self):
        """ DELETE can update conditionally """
        table = self.make_table(index='ts')
        self.query("INSERT INTO foobar (id, bar) VALUES ('a', 1), ('b', 2)")
        self.query("DELETE FROM foobar WHERE id = 'a' and bar = 1")
        items = list(self.dynamo.scan(table))
        self.assertItemsEqual(items, [{'id': 'b', 'bar': 2}])

    def test_delete_in(self):
        """ DELETE can specify KEYS IN """
        table = self.make_table(index='ts')
        self.query("INSERT INTO foobar (id, bar) VALUES ('a', 1), ('b', 2)")
        self.query("DELETE FROM foobar KEYS IN ('a', 1)")
        items = list(self.dynamo.scan(table))
        self.assertItemsEqual(items, [{'id': 'b', 'bar': 2}])

    def test_delete_in_filter(self):
        """ DELETE can specify KEYS IN with WHERE """
        table = self.make_table(range_key=None)
        self.query("INSERT INTO foobar (id, bar) VALUES ('a', 1), ('b', 2)")
        self.query("DELETE FROM foobar KEYS IN 'a', 'b' WHERE bar = 1")
        items = list(self.dynamo.scan(table))
        self.assertItemsEqual(items, [{'id': 'b', 'bar': 2}])

    def test_delete_smart_index(self):
        """ DELETE statement auto-selects correct index name """
        table = self.make_table(index='ts')
        self.query("INSERT INTO foobar (id, bar, ts) VALUES ('a', 1, 100), "
                   "('a', 2, 200)")
        self.query("DELETE FROM foobar WHERE id = 'a' "
                   "and ts > 150")
        results = list(self.dynamo.scan(table))
        self.assertItemsEqual(results, [{'id': 'a', 'bar': 1, 'ts': 100}])

    def test_delete_using(self):
        """ DELETE statement can specify an index """
        table = self.make_table(index='ts')
        self.query("INSERT INTO foobar (id, bar, ts) VALUES ('a', 1, 0), "
                   "('a', 2, 5)")
        self.query("DELETE FROM foobar WHERE id = 'a' and ts < 8 "
                   "USING ts-index")
        items = list(self.dynamo.scan(table))
        self.assertEqual(len(items), 0)

    def test_explain_delete_query(self):
        """ EXPLAIN DELETE query """
        self.make_table()
        self.query("EXPLAIN DELETE FROM foobar WHERE id = 'a'")
        ret = self.engine._call_list
        self.assertEqual(len(ret), 2)
        query, update = ret
        self.assertEqual(query[0], 'query')
        self.assertEqual(update[0], 'delete_item')

    def test_explain_delete_get(self):
        """ EXPLAIN DELETE batch get item """
        self.make_table(range_key=None)
        self.query("EXPLAIN DELETE FROM foobar KEYS IN 'a', 'b'")
        ret = self.engine._call_list
        self.assertEqual(len(ret), 1)
        self.assertEqual(ret[0][0], 'delete_item')

    def test_explain_delete_scan(self):
        """ EXPLAIN DELETE scan """
        self.make_table(range_key=None)
        self.query("EXPLAIN DELETE FROM foobar")
        ret = self.engine._call_list
        self.assertEqual(len(ret), 2)
        scan, update = ret
        self.assertEqual(scan[0], 'scan')
        self.assertEqual(update[0], 'delete_item')
