""" Tests for queries """
from boto.exception import JSONResponseError
from dql.engine import Binary, Table
from dql.models import TableField, IndexField, GlobalIndex

from . import BaseSystemTest


class TestQueries(BaseSystemTest):

    """ System tests for queries """

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

    def test_insert_binary(self):
        """ INSERT statement can insert binary values """
        self.query("CREATE TABLE foobar (id BINARY HASH KEY)")
        self.query("INSERT INTO foobar (id) VALUES (b'a')")
        table = Table('foobar', connection=self.dynamo)
        items = [dict(i) for i in table.scan()]
        self.assertEqual(items, [{'id': Binary(b'a')}])

    def test_count(self):
        """ COUNT statement counts items """
        self.make_table()
        self.query("INSERT INTO foobar (id, bar) VALUES ('a', 1), "
                   "('a', 2)")
        count = self.query("COUNT foobar WHERE id = 'a' ")
        self.assertEquals(count, 2)

    def test_count_smart_index(self):
        """ COUNT statement auto-selects correct index name """
        self.make_table(index='ts')
        self.query("INSERT INTO foobar (id, bar, ts) VALUES ('a', 1, 100), "
                   "('a', 2, 200)")
        count = self.query("COUNT foobar WHERE id = 'a' and ts < 150")
        self.assertEquals(count, 1)

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

    def test_delete_smart_index(self):
        """ DELETE statement auto-selects correct index name """
        table = self.make_table(index='ts')
        self.query("INSERT INTO foobar (id, bar, ts) VALUES ('a', 1, 100), "
                   "('a', 2, 200)")
        self.query("DELETE FROM foobar WHERE id = 'a' "
                   "and ts > 150")
        results = table.scan()
        results = [dict(r) for r in results]
        self.assertItemsEqual(results, [{'id': 'a', 'bar': 1, 'ts': 100}])

    def test_delete_using(self):
        """ DELETE statement can specify an index """
        table = self.make_table(index='ts')
        self.query("INSERT INTO foobar (id, bar, ts) VALUES ('a', 1, 0), "
                   "('a', 2, 5)")
        self.query("DELETE FROM foobar WHERE id = 'a' and ts < 8 "
                   "USING 'ts-index'")
        items = [dict(i) for i in table.scan()]
        self.assertEqual(len(items), 0)

    def test_dump(self):
        """ DUMP SCHEMA generates 'create' statements """
        self.query("CREATE TABLE test (id STRING HASH KEY, bar NUMBER RANGE "
                   "KEY, ts NUMBER INDEX('ts-index'), "
                   "baz STRING KEYS INDEX('baz-index'), "
                   "bag NUMBER INCLUDE INDEX('bag-index', ['foo']), "
                   "THROUGHPUT (2, 6)) "
                   "GLOBAL INDEX ('myindex', bar, baz, THROUGHPUT (1, 2)) "
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
        try:
            self.engine.describe('test', True)
        except JSONResponseError as e:
            self.assertEquals(e.status, 400)
        else:
            assert False, "The test table should not exist"

    def test_multiple_statements(self):
        """ Engine can execute multiple queries separated by ';' """
        result = self.engine.execute("""
            CREATE TABLE test (id STRING HASH KEY);
            INSERT INTO test (id, foo) VALUES ('a', 1), ('b', 2);
            SCAN test
        """)
        scan_result = [dict(r) for r in result]
        self.assertItemsEqual(scan_result, [{'id': 'a', 'foo': 1},
                                            {'id': 'b', 'foo': 2}])


class TestSelect(BaseSystemTest):

    """ Tests for SELECT """

    def test_hash_key(self):
        """ SELECT statement filters by hash key """
        self.make_table()
        self.query("INSERT INTO foobar (id, bar) VALUES ('a', 1), ('b', 2)")
        results = self.query("SELECT * FROM foobar WHERE id = 'a'")
        results = [dict(r) for r in results]
        self.assertItemsEqual(results, [{'id': 'a', 'bar': 1}])

    def test_hash_range(self):
        """ SELECT statement filters by hash and range keys """
        self.make_table()
        self.query("INSERT INTO foobar (id, bar) VALUES ('a', 1), ('b', 2)")
        results = self.query("SELECT * FROM foobar WHERE id = 'a' and bar = 1")
        results = [dict(r) for r in results]
        self.assertItemsEqual(results, [{'id': 'a', 'bar': 1}])

    def test_get(self):
        """ SELECT statement can fetch items directly """
        self.make_table()
        self.query("INSERT INTO foobar (id, bar) VALUES ('a', 1), ('b', 2)")
        results = self.query("SELECT * FROM foobar WHERE "
                             "KEYS IN ('a', 1), ('b', 2)")
        results = [dict(r) for r in results]
        self.assertItemsEqual(results, [{'id': 'a', 'bar': 1},
                                        {'id': 'b', 'bar': 2}])

    def test_reverse(self):
        """ SELECT can reverse order of results """
        self.make_table()
        self.query("INSERT INTO foobar (id, bar) VALUES ('a', 1), ('a', 2)")
        results = self.query("SELECT * FROM foobar WHERE id = 'a' ASC")
        rev_results = self.query("SELECT * FROM foobar WHERE id = 'a' DESC")
        results = [dict(r) for r in results]
        rev_results = [dict(r) for r in reversed(list(rev_results))]
        self.assertEquals(results, rev_results)

    def test_hash_index(self):
        """ SELECT statement filters by indexes """
        self.make_table(index='ts')
        self.query("INSERT INTO foobar (id, bar, ts) VALUES ('a', 1, 100), "
                   "('a', 2, 200)")
        results = self.query("SELECT * FROM foobar WHERE id = 'a' "
                             "and ts < 150 USING 'ts-index'")
        results = [dict(r) for r in results]
        self.assertItemsEqual(results, [{'id': 'a', 'bar': 1, 'ts': 100}])

    def test_smart_index(self):
        """ SELECT statement auto-selects correct index name """
        self.make_table(index='ts')
        self.query("INSERT INTO foobar (id, bar, ts) VALUES ('a', 1, 100), "
                   "('a', 2, 200)")
        results = self.query("SELECT * FROM foobar WHERE id = 'a' "
                             "and ts < 150")
        results = [dict(r) for r in results]
        self.assertItemsEqual(results, [{'id': 'a', 'bar': 1, 'ts': 100}])

    def test_smart_global_index(self):
        """ SELECT statement auto-selects correct global index name """
        self.query("CREATE TABLE foobar (id STRING HASH KEY, foo STRING "
                   "RANGE KEY, bar NUMBER INDEX('bar-index'), baz STRING) "
                   "GLOBAL INDEX ('gindex', baz)")
        self.query("INSERT INTO foobar (id, foo, bar, baz) VALUES "
                   "('a', 'a', 1, 'a'), ('b', 'b', 2, 'b')")
        results = self.query("SELECT * FROM foobar WHERE baz = 'a'")
        results = [dict(r) for r in results]
        self.assertItemsEqual(results, [{'id': 'a', 'foo': 'a',
                                         'bar': 1, 'baz': 'a'}])

    def test_limit(self):
        """ SELECT statement should be able to specify limit """
        self.make_table(index='ts')
        self.query("INSERT INTO foobar (id, bar, ts) VALUES ('a', 1, 100), "
                   "('a', 2, 200)")
        results = self.query("SELECT * FROM foobar WHERE id = 'a' LIMIT 1")
        self.assertEquals(len(list(results)), 1)

    def test_attrs(self):
        """ SELECT statement can fetch only certain attrs """
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
                             "WHERE id = 1 AND bar BEGINS WITH 'a'")
        results = [dict(r) for r in results]
        self.assertItemsEqual(results, [{'id': 1, 'bar': 'abc'}])

    def test_between(self):
        """ SELECT can filter attrs that are between values"""
        self.make_table()
        self.query("INSERT INTO foobar (id, bar) VALUES "
                   "('a', 5), ('a', 10)")
        results = self.query("SELECT * FROM foobar "
                             "WHERE id = 'a' AND bar BETWEEN (1, 8)")
        results = [dict(r) for r in results]
        self.assertItemsEqual(results, [{'id': 'a', 'bar': 5}])


class TestScan(BaseSystemTest):

    """ Tests for SCAN """

    def test(self):
        """ SCAN statement gets all results in a table """
        self.make_table()
        self.query("INSERT INTO foobar (id, bar) VALUES ('a', 1), ('b', 2)")
        results = self.query("SCAN foobar")
        results = [dict(r) for r in results]
        self.assertItemsEqual(results, [{'id': 'a', 'bar': 1},
                                        {'id': 'b', 'bar': 2}])

    def test_filter(self):
        """ SCAN statement can filter results """
        self.make_table()
        self.query("INSERT INTO foobar (id, bar) VALUES ('a', 1), ('b', 2)")
        results = self.query("SCAN foobar FILTER id = 'a'")
        results = [dict(r) for r in results]
        self.assertItemsEqual(results, [{'id': 'a', 'bar': 1}])

    def test_limit(self):
        """ SCAN statement can filter results """
        self.make_table()
        self.query("INSERT INTO foobar (id, bar) VALUES ('a', 1), ('b', 2)")
        results = self.query("SCAN foobar LIMIT 1")
        self.assertEquals(len(list(results)), 1)

    def test_begins_with(self):
        """ SCAN can filter attrs that begin with a string """
        self.query("CREATE TABLE foobar (id NUMBER HASH KEY, "
                   "bar STRING RANGE KEY)")
        self.query("INSERT INTO foobar (id, bar) VALUES "
                   "(1, 'abc'), (1, 'def')")
        results = self.query("SCAN foobar "
                             "FILTER id = 1 AND bar BEGINS WITH 'a'")
        results = [dict(r) for r in results]
        self.assertItemsEqual(results, [{'id': 1, 'bar': 'abc'}])

    def test_between(self):
        """ SCAN can filter attrs that are between values"""
        self.make_table()
        self.query("INSERT INTO foobar (id, bar) VALUES "
                   "('a', 5), ('a', 10)")
        results = self.query("SCAN foobar "
                             "FILTER id = 'a' AND bar BETWEEN (1, 8)")
        results = [dict(r) for r in results]
        self.assertItemsEqual(results, [{'id': 'a', 'bar': 5}])

    def test_null(self):
        """ SCAN can filter if an attr is null """
        self.make_table()
        self.query("INSERT INTO foobar (id, bar) VALUES ('a', 5)")
        self.query("INSERT INTO foobar (id, bar, baz) VALUES ('a', 1, 1)")
        results = self.query("SCAN foobar "
                             "FILTER id = 'a' AND baz IS NULL")
        results = [dict(r) for r in results]
        self.assertItemsEqual(results, [{'id': 'a', 'bar': 5}])

    def test_not_null(self):
        """ SCAN can filter if an attr is not null """
        self.make_table()
        self.query("INSERT INTO foobar (id, bar) VALUES ('a', 5)")
        self.query("INSERT INTO foobar (id, bar, baz) VALUES ('a', 1, 1)")
        results = self.query("SCAN foobar "
                             "FILTER id = 'a' AND baz IS NOT NULL")
        results = [dict(r) for r in results]
        self.assertItemsEqual(results, [{'id': 'a', 'bar': 1, 'baz': 1}])

    def test_in(self):
        """ SCAN can filter if an attr is in a set """
        self.make_table()
        self.query("INSERT INTO foobar (id, bar) VALUES ('a', 5), ('a', 2)")
        results = self.query("SCAN foobar "
                             "FILTER id = 'a' AND bar IN (1, 3, 5)")
        results = [dict(r) for r in results]
        self.assertItemsEqual(results, [{'id': 'a', 'bar': 5}])

    def test_contains(self):
        """ SCAN can filter if a set contains an item """
        self.make_table()
        self.query("INSERT INTO foobar (id, bar, baz) VALUES "
                   "('a', 5, (1, 2, 3)), ('a', 1, (4, 5, 6))")
        results = self.query("SCAN foobar "
                             "FILTER id = 'a' AND baz CONTAINS 2")
        results = [dict(r) for r in results]
        self.assertItemsEqual(results, [{'id': 'a', 'bar': 5,
                                         'baz': set([1, 2, 3])}])

    def test_not_contains(self):
        """ SCAN can filter if a set contains an item """
        self.make_table()
        self.query("INSERT INTO foobar (id, bar, baz) VALUES "
                   "('a', 5, (1, 2, 3)), ('a', 1, (4, 5, 6))")
        results = self.query("SCAN foobar "
                             "FILTER id = 'a' AND baz NOT CONTAINS 5")
        results = [dict(r) for r in results]
        self.assertItemsEqual(results, [{'id': 'a', 'bar': 5,
                                         'baz': set([1, 2, 3])}])


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
        gindex = GlobalIndex('myindex', 'ALL', 'ACTIVE', hash_key, range_key, 1, 2, 0,
                             0)
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
        gindex = GlobalIndex('myindex', 'ALL', 'ACTIVE', hash_key, None, 1, 2, 0, 0)
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
        gindex = GlobalIndex('myindex', 'KEYS', 'ACTIVE', hash_key, None, 1, 2,
                             0, 0)
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
        gindex = GlobalIndex('myindex', 'INCLUDE', 'ACTIVE', hash_key, None, 1, 2, 0, 0, ['bar', 'baz'])
        self.assertEquals(desc.global_indexes, {
            'myindex': gindex,
        })


class TestUpdate(BaseSystemTest):

    """ Tests for UPDATE """

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
        self.query(
            "UPDATE foobar SET baz = 3 WHERE KEYS IN ('a', 1), ('b', 2)")
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

    def test_update_add(self):
        """ UPDATE can add elements to set """
        table = self.make_table()
        self.query("INSERT INTO foobar (id, bar, baz) VALUES ('a', 1, ())")
        self.query("UPDATE foobar SET baz << 2")
        self.query("UPDATE foobar SET baz << (1, 3)")
        items = [dict(i) for i in table.scan()]
        self.assertItemsEqual(items, [{'id': 'a', 'bar': 1,
                                       'baz': set([1, 2, 3])}])

    def test_update_remove(self):
        """ UPDATE can remove elements from set """
        table = self.make_table()
        self.query("INSERT INTO foobar (id, bar, baz) VALUES "
                   "('a', 1, (1, 2, 3, 4))")
        self.query("UPDATE foobar SET baz >> 2")
        self.query("UPDATE foobar SET baz >> (1, 3)")
        items = [dict(i) for i in table.scan()]
        self.assertItemsEqual(items, [{'id': 'a', 'bar': 1, 'baz': set([4])}])

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

    def test_update_expression(self):
        """ UPDATE python expressions can reference item attributes """
        self.make_table()
        self.query("INSERT INTO foobar (id, bar, baz) VALUES ('a', 1, 10), "
                   "('b', 2, 20)")
        self.query("UPDATE foobar SET baz = `bar + 1`")
        result = self.query('SCAN foobar')
        items = [dict(i) for i in result]
        self.assertItemsEqual(items, [{'id': 'a', 'bar': 1, 'baz': 2},
                                      {'id': 'b', 'bar': 2, 'baz': 3}])

    def test_update_expression_defaults(self):
        """ UPDATE python expressions can reference row directly """
        self.make_table()
        self.query("INSERT INTO foobar (id, bar, baz) VALUES ('a', 1, 1), "
                   "('b', 2, null)")
        code = '\n'.join((
            "if row.get('baz') is not None:",
            "    return baz + 5"
        ))
        self.query("UPDATE foobar SET baz = m`%s`" % code)
        result = self.query('SCAN foobar')
        items = [dict(i) for i in result]
        self.assertItemsEqual(items, [{'id': 'a', 'bar': 1, 'baz': 6},
                                      {'id': 'b', 'bar': 2}])
