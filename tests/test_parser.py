""" Tests for the language parser """
try:
    from unittest2 import TestCase  # pylint: disable=F0401
except ImportError:
    from unittest import TestCase
from pyparsing import ParseException

from dql.grammar import statement_parser, parser
from dql.grammar.query import where, select_where, filter_, value


TEST_CASES = {
    'select': [
        ('SELECT * FROM foobars WHERE foo = 0', ['SELECT', ['*'], 'FROM', 'foobars', 'WHERE', [['foo', '=', ['0']]]]),
        ('SELECT CONSISTENT * FROM foobars WHERE foo = 0', ['SELECT', 'CONSISTENT', ['*'], 'FROM', 'foobars', 'WHERE', [['foo', '=', ['0']]]]),
        ('SELECT * FROM foobars WHERE foo = 0 DESC', ['SELECT', ['*'], 'FROM', 'foobars', 'WHERE', [['foo', '=', ['0']]], 'DESC']),
        ('SELECT * FROM foobars WHERE foo = 0 and bar = "green"', ['SELECT', ['*'], 'FROM', 'foobars', 'WHERE', [['foo', '=', ['0']], ['bar', '=', ['"green"']]]]),
        ('SELECT * FROM foobars', 'error'),
        ('SELECT * foobars WHERE foo = 0', 'error'),
        ('SELECT * FROM foobars WHERE foo != 0', 'error'),
        ('SELECT * FROM "foobars" WHERE foo = 0', 'error'),
        ('SELECT * FROM foobars WHERE foo = 0 garbage', 'error'),
    ],
    'select_in': [
        ('SELECT * FROM foobars WHERE KEYS IN ("hash1"), ("hash2")', ['SELECT', ['*'], 'FROM', 'foobars', 'WHERE', 'KEYS', 'IN', [[['"hash1"']], [['"hash2"']]]]),
        ('SELECT * FROM foobars WHERE KEYS IN ("hash1", "range1"), ("hash2", "range2")', ['SELECT', ['*'], 'FROM', 'foobars', 'WHERE', 'KEYS', 'IN', [[['"hash1"'], ['"range1"']], [['"hash2"'], ['"range2"']]]]),
    ],
    'select_using': [
        ('SELECT * FROM foobars WHERE foo = 0 USING my_index', ['SELECT', ['*'], 'FROM', 'foobars', 'WHERE', [['foo', '=', ['0']]], 'USING', ['my_index']]),
        ('SELECT * FROM foobars WHERE foo = 0 AND bar < 4 USING my_index', ['SELECT', ['*'], 'FROM', 'foobars', 'WHERE', [['foo', '=', ['0']], ['bar', '<', ['4']]], 'USING', ['my_index']]),
    ],
    'select_limit': [
        ('SELECT * FROM foobars WHERE foo = 0 LIMIT 5', ['SELECT', ['*'], 'FROM', 'foobars', 'WHERE', [['foo', '=', ['0']]], ['LIMIT', ['5']]]),
        ('SELECT * FROM foobars WHERE foo = 0 USING my_index LIMIT 2', ['SELECT', ['*'], 'FROM', 'foobars', 'WHERE', [['foo', '=', ['0']]], 'USING', ['my_index'], ['LIMIT', ['2']]]),
        ('SELECT * FROM foobars WHERE foo > 0 LIMIT 4 garbage', 'error'),
    ],
    'select_attrs': [
        ('SELECT foo, bar FROM foobars WHERE foo = 0', ['SELECT', ['foo', 'bar'], 'FROM', 'foobars', 'WHERE', [['foo', '=', ['0']]]]),
        ('SELECT (foo, bar) FROM foobars WHERE foo = 0', ['SELECT', ['foo', 'bar'], 'FROM', 'foobars', 'WHERE', [['foo', '=', ['0']]]]),
        ('SELECT foo, bar FROM foobars WHERE foo = 0 and bar = "green"', ['SELECT', ['foo', 'bar'], 'FROM', 'foobars', 'WHERE', [['foo', '=', ['0']], ['bar', '=', ['"green"']]]]),
    ],
    'scan': [
        ('SCAN foobars', ['SCAN', 'foobars']),
        ('SCAN foobars FILTER foo = 0', ['SCAN', 'foobars', 'FILTER', [['foo', '=', ['0']]]]),
        ('SCAN foobars FILTER foo = 0 and bar != "green"', ['SCAN', 'foobars', 'FILTER', [['foo', '=', ['0']], ['bar', '!=', ['"green"']]]]),
        ('SCAN "foobars"', 'error'),
        ('SCAN foobars garbage', 'error'),
    ],
    'count': [
        ('COUNT foobars WHERE foo = 0', ['COUNT', 'foobars', 'WHERE', [['foo', '=', ['0']]]]),
        ('COUNT CONSISTENT foobars WHERE foo = 0', ['COUNT', 'CONSISTENT', 'foobars', 'WHERE', [['foo', '=', ['0']]]]),
        ('COUNT foobars WHERE foo = 0 and bar = "green"', ['COUNT', 'foobars', 'WHERE', [['foo', '=', ['0']], ['bar', '=', ['"green"']]]]),
        ('COUNT foobars', 'error'),
        ('COUNT WHERE foo = 0', 'error'),
        ('COUNT "foobars" WHERE foo = 0', 'error'),
        ('COUNT foobars WHERE foo = 0 garbage', 'error'),
    ],
    'count_using': [
        ('COUNT foobars WHERE foo = 0 USING my_index', ['COUNT', 'foobars', 'WHERE', [['foo', '=', ['0']]], 'USING', ['my_index']]),
        ('COUNT foobars WHERE foo = 0 AND bar < 4 USING my_index', ['COUNT', 'foobars', 'WHERE', [['foo', '=', ['0']], ['bar', '<', ['4']]], 'USING', ['my_index']]),
    ],
    'delete': [
        ('DELETE FROM foobars WHERE foo = 0', ['DELETE', 'FROM', 'foobars', 'WHERE', [['foo', '=', ['0']]]]),
        ('DELETE FROM foobars WHERE foo = 0 and bar = "green"', ['DELETE', 'FROM', 'foobars', 'WHERE', [['foo', '=', ['0']], ['bar', '=', ['"green"']]]]),
        ('DELETE FROM foobars WHERE foo = 0 USING my_index', ['DELETE', 'FROM', 'foobars', 'WHERE', [['foo', '=', ['0']]], 'USING', ['my_index']]),
        ('DELETE FROM foobars WHERE foo = 0 AND bar = 4 USING my_index', ['DELETE', 'FROM', 'foobars', 'WHERE', [['foo', '=', ['0']], ['bar', '=', ['4']]], 'USING', ['my_index']]),
        ('DELETE FROM foobars', 'error'),
        ('DELETE foobars WHERE foo = 0', 'error'),
        ('DELETE FROM "foobars" WHERE foo = 0', 'error'),
        ('DELETE FROM foobars WHERE foo = 0 garbage', 'error'),
    ],
    'delete_in': [
        ('DELETE FROM foobars WHERE KEYS IN ("hash1"), ("hash2")', ['DELETE', 'FROM', 'foobars', 'WHERE', 'KEYS', 'IN', [[['"hash1"']], [['"hash2"']]]]),
        ('DELETE FROM foobars WHERE KEYS IN ("hash1", "range1"), ("hash2", "range2")', ['DELETE', 'FROM', 'foobars', 'WHERE', 'KEYS', 'IN', [[['"hash1"'], ['"range1"']], [['"hash2"'], ['"range2"']]]]),
    ],
    'update': [
        ('UPDATE foobars SET foo = 0, bar += 3', ['UPDATE', 'foobars', 'SET', [['foo', '=', ['0']], ['bar', '+=', ['3']]]]),
        ('UPDATE foobars SET foo << 0, bar >> ("a", "b")', ['UPDATE', 'foobars', 'SET', [['foo', '<<', ['0']], ['bar', '>>', [['"a"'], ['"b"']]]]]),
        ('UPDATE foobars SET foo = 0 WHERE KEYS IN ("a"), ("b")', ['UPDATE', 'foobars', 'SET', [['foo', '=', ['0']]], 'WHERE', 'KEYS', 'IN', [[['"a"']], [['"b"']]]]),
        ('UPDATE foobars SET foo = 0 WHERE foo = 3', ['UPDATE', 'foobars', 'SET', [['foo', '=', ['0']]], 'WHERE', [['foo', '=', ['3']]]]),
        ('UPDATE foobars SET foo = 0, bar = NULL', ['UPDATE', 'foobars', 'SET', [['foo', '=', ['0']], ['bar', '=', ['NULL']]]]),
        ('UPDATE foobars SET foo = 0 RETURNS ALL OLD', ['UPDATE', 'foobars', 'SET', [['foo', '=', ['0']]], 'RETURNS', ['ALL', 'OLD']]),
        ('UPDATE foobars SET foo *= 0', 'error'),
        ('UPDATE foobars SET foo = 0 RETURNS garbage', 'error'),
    ],
    'create': [
        ('CREATE TABLE foobars (foo string hash key)', ['CREATE', 'TABLE', 'foobars', [['foo', 'STRING', ['HASH', 'KEY']]]]),
        ('CREATE TABLE foobars (foo string hash key, bar NUMBER)', ['CREATE', 'TABLE', 'foobars', [['foo', 'STRING', ['HASH', 'KEY']], ['bar', 'NUMBER']]]),
        ('CREATE TABLE foobars (foo string hash key, THROUGHPUT (1, 1))', ['CREATE', 'TABLE', 'foobars', [['foo', 'STRING', ['HASH', 'KEY']]], 'THROUGHPUT', [['1'], ['1']]]),
        ('CREATE TABLE IF NOT EXISTS foobars (foo string hash key)', ['CREATE', 'TABLE', ['IF', 'NOT', 'EXISTS'], 'foobars', [['foo', 'STRING', ['HASH', 'KEY']]]]),
        ('CREATE TABLE foobars (foo string hash key, bar number range key)', ['CREATE', 'TABLE', 'foobars', [['foo', 'STRING', ['HASH', 'KEY']], ['bar', 'NUMBER', ['RANGE', 'KEY']]]]),
        ('CREATE TABLE foobars (foo binary index("foo-index"))', ['CREATE', 'TABLE', 'foobars', [['foo', 'BINARY', ['INDEX', ['"foo-index"']]]]]),
        ('CREATE TABLE foobars (foo binary index(idxname))', ['CREATE', 'TABLE', 'foobars', [['foo', 'BINARY', ['INDEX', ['idxname']]]]]),
        ('CREATE foobars (foo binary index(idxname))', 'error'),
        ('CREATE TABLE foobars foo binary hash key', 'error'),
        ('CREATE TABLE foobars (foo hash key)', 'error'),
        ('CREATE TABLE foobars (foo binary hash key) garbage', 'error'),
    ],
    'create_global': [
        ('CREATE TABLE foobars (foo string hash key) GLOBAL INDEX ("gindex", foo)', ['CREATE', 'TABLE', 'foobars', [['foo', 'STRING', ['HASH', 'KEY']]], [[['"gindex"'], 'foo']]]),
        ('CREATE TABLE foobars (foo string hash key) GLOBAL INDEX ("gindex", foo, bar)', ['CREATE', 'TABLE', 'foobars', [['foo', 'STRING', ['HASH', 'KEY']]], [[['"gindex"'], 'foo', 'bar']]]),
        ('CREATE TABLE foobars (foo string hash key) GLOBAL INDEX ("gindex", foo), ("g2idx", bar, foo)', ['CREATE', 'TABLE', 'foobars', [['foo', 'STRING', ['HASH', 'KEY']]], [[['"gindex"'], 'foo'], [['"g2idx"'], 'bar', 'foo']]]),
        ('CREATE TABLE foobars (foo string hash key) GLOBAL INDEX ("gindex", foo, bar, THROUGHPUT (2, 4))', ['CREATE', 'TABLE', 'foobars', [['foo', 'STRING', ['HASH', 'KEY']]], [[['"gindex"'], 'foo', 'bar', ['THROUGHPUT', [['2'], ['4']]]]]]),
        ('CREATE TABLE foobars (foo string hash key) GLOBAL INDEX ("gindex", foo, THROUGHPUT (2, 4))', ['CREATE', 'TABLE', 'foobars', [['foo', 'STRING', ['HASH', 'KEY']]], [[['"gindex"'], 'foo', ['THROUGHPUT', [['2'], ['4']]]]]]),
        ('CREATE TABLE foobars (foo string hash key) GLOBAL INDEX ("gindex")', 'error'),
        ('CREATE TABLE foobars (foo string hash key) GLOBAL INDEX ("gindex", foo, bar, baz)', 'error'),
        ('CREATE TABLE foobars (foo string hash key) GLOBAL INDEX ("gindex", foo, bar),', 'error'),
    ],
    'insert': [
        ('INSERT INTO foobars (foo, bar) VALUES (1, 2)', ['INSERT', 'INTO', 'foobars', ['foo', 'bar'], 'VALUES', [[['1'], ['2']]]]),
        ('INSERT INTO foobars (foo, bar) VALUES (1, 2), (3, 4)', ['INSERT', 'INTO', 'foobars', ['foo', 'bar'], 'VALUES', [[['1'], ['2']], [['3'], ['4']]]]),
        ('INSERT INTO foobars (foo, bar) VALUES (b"binary", ("set", "of", "values"))', ['INSERT', 'INTO', 'foobars', ['foo', 'bar'], 'VALUES', [[['b"binary"'], [['"set"'], ['"of"'], ['"values"']]]]]),
        ('INSERT foobars (foo, bar) VALUES (1, 2)', 'error'),
        ('INSERT INTO foobars foo, bar VALUES (1, 2)', 'error'),
        ('INSERT INTO foobars (foo, bar) VALUES', 'error'),
        ('INSERT INTO foobars (foo, bar) VALUES 1, 2', 'error'),
        ('INSERT INTO foobars (foo, bar) VALUES (1, 2) garbage', 'error'),
    ],
    'drop': [
        ('DROP TABLE foobars', ['DROP', 'TABLE', 'foobars']),
        ('DROP TABLE IF EXISTS foobars', ['DROP', 'TABLE', ['IF', 'EXISTS'], 'foobars']),
        ('DROP foobars', 'error'),
        ('DROP TABLE foobars garbage', 'error'),
    ],
    'alter': [
        ('ALTER TABLE foobars SET THROUGHPUT (3, 4)', ['ALTER', 'TABLE', 'foobars', 'SET', 'THROUGHPUT', [['3'], ['4']]]),
        ('ALTER TABLE foobars SET INDEX foo THROUGHPUT (3, 4)', ['ALTER', 'TABLE', 'foobars', 'SET', 'INDEX', 'foo', 'THROUGHPUT', [['3'], ['4']]]),
        ('ALTER TABLE foobars SET foo = bar', 'error'),
        ('ALTER TABLE foobars SET THROUGHPUT 1, 1', 'error'),
    ],
    'dump': [
        ('DUMP SCHEMA', ['DUMP', 'SCHEMA']),
        ('DUMP SCHEMA foobars, wibbles', ['DUMP', 'SCHEMA', ['foobars', 'wibbles']]),
        ('DUMP SCHEMA foobars wibbles', 'error'),
    ],
    'multiple': [
        ('DUMP SCHEMA;DUMP SCHEMA', [['DUMP', 'SCHEMA'], ['DUMP', 'SCHEMA']]),
        ('DUMP SCHEMA;\nDUMP SCHEMA', [['DUMP', 'SCHEMA'], ['DUMP', 'SCHEMA']]),
        ('DUMP SCHEMA\n;\nDUMP SCHEMA', [['DUMP', 'SCHEMA'], ['DUMP', 'SCHEMA']]),
    ],
    'where': [
        ('WHERE foo = 1 AND bar > 1', ['WHERE', [['foo', '=', ['1']], ['bar', '>', ['1']]]]),
        ('WHERE foo >= 1 AND bar < 1', ['WHERE', [['foo', '>=', ['1']], ['bar', '<', ['1']]]]),
        ('WHERE foo <= 1', ['WHERE', [['foo', '<=', ['1']]]]),
        ('WHERE foo BEGINS WITH "flap"', ['WHERE', [['foo', 'BEGINS WITH', ['"flap"']]]]),
        ('WHERE foo BETWEEN (1, 5)', ['WHERE', [['foo', 'BETWEEN', [['1'], ['5']]]]]),
        ('WHERE foo != 1', 'error'),
        ('WHERE foo BETWEEN 1', 'error'),
        ('WHERE foo BETWEEN (1, 2, 3)', 'error'),
    ],
    'select_where': [
        ('WHERE foo = 1 AND bar > 1', ['WHERE', [['foo', '=', ['1']], ['bar', '>', ['1']]]]),
        ('WHERE KEYS IN (1)', ['WHERE', 'KEYS', 'IN', [[['1']]]]),
        ('WHERE KEYS IN (1, 2), (3, 4)', ['WHERE', 'KEYS', 'IN', [[['1'], ['2']], [['3'], ['4']]]]),
        ('WHERE KEYS IN (1, 2, 3)', 'error'),
    ],
    'filter': [
        ('FILTER foo = 1 AND bar > 1', ['FILTER', [['foo', '=', ['1']], ['bar', '>', ['1']]]]),
        ('FILTER foo >= 1 AND bar < 1', ['FILTER', [['foo', '>=', ['1']], ['bar', '<', ['1']]]]),
        ('FILTER foo <= 1 AND bar != 1', ['FILTER', [['foo', '<=', ['1']], ['bar', '!=', ['1']]]]),
        ('FILTER foo IS NULL AND bar IS NOT NULL', ['FILTER', [['foo', 'IS', 'NULL'], ['bar', 'IS', 'NOT NULL']]]),
        ('FILTER foo BEGINS WITH "flap"', ['FILTER', [['foo', 'BEGINS WITH', ['"flap"']]]]),
        ('FILTER foo CONTAINS 1', ['FILTER', [['foo', 'CONTAINS', ['1']]]]),
        ('FILTER foo NOT CONTAINS 1', ['FILTER', [['foo', 'NOT CONTAINS', ['1']]]]),
        ('FILTER foo IN (1, 2)', ['FILTER', [['foo', 'IN', [['1'], ['2']]]]]),
        ('FILTER foo BETWEEN (1, 5)', ['FILTER', [['foo', 'BETWEEN', [['1'], ['5']]]]]),
        ('FILTER foo BETWEEN 1', 'error'),
        ('FILTER foo BETWEEN (1, 2, 3)', 'error'),
        ('FILTER foo IN "hi"', 'error'),
    ],
    'variables': [
        ('"a"', [['"a"']]),
        ('1', [['1']]),
        ('2.7', [['2.7']]),
        ('b"hi"', [['b"hi"']]),
        ('null', [['NULL']]),
        ('()', [['()']]),
        ('(1, 2)', [[['1'], ['2']]]),
        ('("a", "b")', [[['"a"'], ['"b"']]]),
    ],
}


class TestParser(TestCase):

    """ Tests for the language parser """

    def _run_tests(self, key, grammar=statement_parser):
        """ Run a set of tests """
        for string, result in TEST_CASES[key]:
            try:
                parse_result = grammar.parseString(string)
                if result == 'error':
                    assert False, ("Parsing '%s' should have failed.\nGot: %s"
                                   % (string, parse_result.asList()))
                else:
                    self.assertEquals(result, parse_result.asList())
            except ParseException as e:
                if result != 'error':
                    print string
                    print ' ' * e.loc + '^'
                    raise

    def test_select(self):
        """ Run tests for SELECT statements """
        self._run_tests('select')

    def test_select_in(self):
        """ SELECT syntax for fetching items by primary key """
        self._run_tests('select_in')

    def test_select_using(self):
        """ SELECT tests that specify an index """
        self._run_tests('select_using')

    def test_select_limit(self):
        """ SELECT tests with the LIMIT clause """
        self._run_tests('select_limit')

    def test_select_attrs(self):
        """ SELECT may fetch only specific attributes """
        self._run_tests('select_attrs')

    def test_scan(self):
        """ Run tests for SCAN statements """
        self._run_tests('scan')

    def test_count(self):
        """ Run tests for COUNT statements """
        self._run_tests('count')

    def test_count_using(self):
        """ COUNT tests that specify an index """
        self._run_tests('count_using')

    def test_delete(self):
        """ Run tests for DELETE statements """
        self._run_tests('delete')

    def test_delete_in(self):
        """ DELETE syntax for fetching items by primary key """
        self._run_tests('delete_in')

    def test_update(self):
        """ Run tests for UPDATE statements """
        self._run_tests('update')

    def test_create(self):
        """ Run tests for CREATE statements """
        self._run_tests('create')

    def test_create_global(self):
        """ Run tests for CREATE statements with global indexes """
        self._run_tests('create_global')

    def test_insert(self):
        """ Run tests for INSERT statements """
        self._run_tests('insert')

    def test_drop(self):
        """ Run tests for DROP statements """
        self._run_tests('drop')

    def test_alter(self):
        """ Run tests for ALTER statements """
        self._run_tests('alter')

    def test_dump(self):
        """ Run tests for DUMP statements """
        self._run_tests('dump')

    def test_multiple_statements(self):
        """ Run tests for multiple-line statements """
        self._run_tests('multiple', parser)

    def test_where(self):
        """ Run tests for the where clause """
        self._run_tests('where', where)

    def test_select_where(self):
        """ Run tests for the where clause on select statements """
        self._run_tests('select_where', select_where)

    def test_filter(self):
        """ Run tests for the filter clause """
        self._run_tests('filter', filter_)

    def test_variables(self):
        """ Run tests for parsing variables """
        self._run_tests('variables', value)
