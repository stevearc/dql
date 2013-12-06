""" Tests for the language parser """
try:
    from unittest2 import TestCase  # pylint: disable=F0401
except ImportError:
    from unittest import TestCase
from pyparsing import ParseException

from .. import parser


TEST_CASES = {
    'select': [
        ('SELECT FROM foobars WHERE foo = 0', ['SELECT', 'FROM', 'foobars', 'where', ['foo', '=', ['0']]]),
        ('SELECT FROM foobars WHERE foo = 0 and bar = "green"', ['SELECT', 'FROM', 'foobars', 'where', ['foo', '=', ['0']], 'AND', ['bar', '=', ['"green"']]]),
        ('SELECT FROM foobars WHERE (foo = 0 and bar = "green")', ['SELECT', 'FROM', 'foobars', 'where', ['(', ['foo', '=', ['0']], 'AND', ['bar', '=', ['"green"']], ')']]),
        ('SELECT FROM foobars', 'error'),
        ('SELECT foobars WHERE foo = 0', 'error'),
        ('SELECT FROM "foobars" WHERE foo = 0', 'error'),
        ('SELECT FROM foobars WHERE foo = 0 garbage', 'error'),
    ],
    'select_using': [
        ('SELECT FROM foobars WHERE foo = 0 USING my_index', ['SELECT', 'FROM', 'foobars', 'where', ['foo', '=', ['0']], 'USING', ['my_index']]),
        ('SELECT FROM foobars WHERE foo = 0 AND bar < 4 USING my_index', ['SELECT', 'FROM', 'foobars', 'where', ['foo', '=', ['0']], 'AND', ['bar', '<', ['4']], 'USING', ['my_index']]),
    ],
    'select_limit': [
        ('SELECT FROM foobars WHERE foo = 0 LIMIT 5', ['SELECT', 'FROM', 'foobars', 'where', ['foo', '=', ['0']], ['LIMIT', ['5']]]),
        ('SELECT FROM foobars WHERE foo = 0 USING my_index LIMIT 2', ['SELECT', 'FROM', 'foobars', 'where', ['foo', '=', ['0']], 'USING', ['my_index'], ['LIMIT', ['2']]]),
        ('SELECT FROM foobars WHERE foo > 0 LIMIT 4 garbage', 'error'),
    ],
    'delete': [
        ('DELETE FROM foobars WHERE foo = 0', ['DELETE', 'FROM', 'foobars', 'where', ['foo', '=', ['0']]]),
        ('DELETE FROM foobars WHERE foo = 0 and bar = "green"', ['DELETE', 'FROM', 'foobars', 'where', ['foo', '=', ['0']], 'AND', ['bar', '=', ['"green"']]]),
        ('DELETE FROM foobars WHERE (foo = 0 and bar = "green")', ['DELETE', 'FROM', 'foobars', 'where', ['(', ['foo', '=', ['0']], 'AND', ['bar', '=', ['"green"']], ')']]),
        ('DELETE FROM foobars WHERE foo = 0 USING my_index', ['DELETE', 'FROM', 'foobars', 'where', ['foo', '=', ['0']], 'USING', ['my_index']]),
        ('DELETE FROM foobars WHERE foo = 0 AND bar = 4 USING my_index', ['DELETE', 'FROM', 'foobars', 'where', ['foo', '=', ['0']], 'AND', ['bar', '=', ['4']], 'USING', ['my_index']]),
        ('DELETE FROM foobars', 'error'),
        ('DELETE foobars WHERE foo = 0', 'error'),
        ('DELETE FROM "foobars" WHERE foo = 0', 'error'),
        ('DELETE FROM foobars WHERE foo = 0 garbage', 'error'),
    ],
    'update': [
    ],
    'create': [
        ('CREATE TABLE foobars (foo string hash key)', ['CREATE', 'TABLE', 'foobars', '(', [['foo', 'STRING', ['HASH', 'KEY']]], ')']),
        ('CREATE TABLE IF NOT EXISTS foobars (foo string hash key)', ['CREATE', 'TABLE', ['IF', 'NOT', 'EXISTS'], 'foobars', '(', [['foo', 'STRING', ['HASH', 'KEY']]], ')']),
        ('CREATE TABLE foobars (foo string hash key, bar number range key)', ['CREATE', 'TABLE', 'foobars', '(', [['foo', 'STRING', ['HASH', 'KEY']], ['bar', 'NUMBER', ['RANGE', 'KEY']]], ')']),
        ('CREATE TABLE foobars (foo binary index("foo-index"))', ['CREATE', 'TABLE', 'foobars', '(', [['foo', 'BINARY', ['INDEX', ['"foo-index"']]]], ')']),
        ('CREATE TABLE foobars (foo binary index(idxname))', ['CREATE', 'TABLE', 'foobars', '(', [['foo', 'BINARY', ['INDEX', ['idxname']]]], ')']),
        ('CREATE foobars (foo binary index(idxname))', 'error'),
        ('CREATE TABLE foobars foo binary hash key', 'error'),
        ('CREATE TABLE foobars (foo binary)', 'error'),
        ('CREATE TABLE foobars (foo hash key)', 'error'),
        ('CREATE TABLE foobars (foo binary hash key) garbage', 'error'),
    ],
    'insert': [
        ('INSERT INTO foobars (foo, bar) VALUES (1, 2)', ['INSERT', 'INTO', 'foobars', ['foo', 'bar'], 'VALUES', [[['1'], ['2']]]]),
        ('INSERT INTO foobars (foo, bar) VALUES (1, 2), (3, 4)', ['INSERT', 'INTO', 'foobars', ['foo', 'bar'], 'VALUES', [[['1'], ['2']], [['3'], ['4']]]]),
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
}


class TestParser(TestCase):

    """ Tests for the language parser """

    def _run_tests(self, key):
        """ Run a set of tests """
        for string, result in TEST_CASES[key]:
            if result == 'error':
                try:
                    parse_result = parser.parseString(string)
                except ParseException:
                    pass
                else:
                    assert False, ("Parsing '%s' should have failed.\nGot: %s"
                                   % (string, parse_result.asList()))
            else:
                parse_result = parser.parseString(string)
                self.assertEquals(result, parse_result.asList())

    def test_select(self):
        """ Run tests for SELECT statements """
        self._run_tests('select')

    def test_select_using(self):
        """ SELECT tests that specify an index """
        self._run_tests('select_using')

    def test_select_limit(self):
        """ SELECT tests with the LIMIT clause """
        self._run_tests('select_limit')

    def test_delete(self):
        """ Run tests for DELETE statements """
        self._run_tests('delete')

    def test_update(self):
        """ Run tests for UPDATE statements """
        self._run_tests('update')

    def test_create(self):
        """ Run tests for CREATE statements """
        self._run_tests('create')

    def test_insert(self):
        """ Run tests for INSERT statements """
        self._run_tests('insert')

    def test_drop(self):
        """ Run tests for DROP statements """
        self._run_tests('drop')
