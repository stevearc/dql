""" Tests for the query engine """
import unittest
from decimal import Decimal

from dynamo3 import Binary
from pyparsing import ParseException

from dql.engine import FragmentEngine

from . import BaseSystemTest


class TestDataTypes(BaseSystemTest):

    """ Make sure we can parse and handle all data types """

    def test_str(self):
        """ Can insert string literals """
        table = self.make_table(range_key=None)
        self.query("INSERT INTO foobar (id) VALUES ('a')")
        result = list(self.dynamo.scan(table))
        self.assertCountEqual(result, [{"id": "a"}])

    def test_int(self):
        """ Can insert integer literals """
        table = self.make_table(range_key=None)
        self.query("INSERT INTO foobar (id, bar) VALUES ('a', 5)")
        result = list(self.dynamo.scan(table))[0]
        self.assertEqual(result["bar"], 5)

    def test_float(self):
        """ Can insert float literals """
        table = self.make_table(range_key=None)
        self.query("INSERT INTO foobar (id, bar) VALUES ('a', 1.2345)")
        result = list(self.dynamo.scan(table))[0]
        self.assertEqual(result["bar"], Decimal("1.2345"))

    def test_bool(self):
        """ Can insert boolean literals """
        table = self.make_table(range_key=None)
        self.query("INSERT INTO foobar (id, bar) VALUES ('a', false)")
        result = list(self.dynamo.scan(table))[0]
        self.assertEqual(result["bar"], False)

    def test_binary(self):
        """ Can insert binary literals """
        table = self.make_table(range_key=None)
        self.query("INSERT INTO foobar (id, bar) VALUES ('a', b'abc')")
        result = list(self.dynamo.scan(table))[0]
        self.assertTrue(isinstance(result["bar"], Binary))
        self.assertEqual(result["bar"], b"abc")

    def test_list(self):
        """ Can insert list literals """
        table = self.make_table(range_key=None)
        self.query("INSERT INTO foobar (id, bar) VALUES ('a', [1, null, 'a'])")
        result = list(self.dynamo.scan(table))[0]
        self.assertEqual(result["bar"], [1, None, "a"])

    def test_empty_list(self):
        """ Can insert empty list literals """
        table = self.make_table(range_key=None)
        self.query("INSERT INTO foobar (id, bar) VALUES ('a', [])")
        result = list(self.dynamo.scan(table))[0]
        self.assertEqual(result["bar"], [])

    def test_nested_list(self):
        """ Can insert nested list literals """
        table = self.make_table(range_key=None)
        self.query("INSERT INTO foobar (id, bar) VALUES ('a', [1, [2, 3]])")
        result = list(self.dynamo.scan(table))[0]
        self.assertEqual(result["bar"], [1, [2, 3]])

    def test_dict(self):
        """ Can insert dict literals """
        table = self.make_table(range_key=None)
        self.query("INSERT INTO foobar (id, bar) VALUES ('a', {'a': 2})")
        result = list(self.dynamo.scan(table))[0]
        self.assertEqual(result["bar"], {"a": 2})

    def test_empty_dict(self):
        """ Can insert empty dict literals """
        table = self.make_table(range_key=None)
        self.query("INSERT INTO foobar (id, bar) VALUES ('a', {})")
        result = list(self.dynamo.scan(table))[0]
        self.assertEqual(result["bar"], {})

    def test_nested_dict(self):
        """ Can insert nested dict literals """
        table = self.make_table(range_key=None)
        self.query("INSERT INTO foobar (id, bar) VALUES ('a', {'a': {'b': null}})")
        result = list(self.dynamo.scan(table))[0]
        self.assertEqual(result["bar"], {"a": {"b": None}})


class TestFragmentEngine(BaseSystemTest):

    """ Tests for the FragmentEngine """

    engine: FragmentEngine

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
        desc = self.engine.describe("test")
        self.assertIsNotNone(desc)

    def test_format_exc(self):
        """ Fragment engine can pretty-format a parse error """
        query = "SELECT * FROM\n\ntable\nWHERE;"
        try:
            for fragment in query.split("\n"):
                self.query(fragment)
        except ParseException as e:
            pretty = self.engine.pformat_exc(e)
            self.assertEqual(pretty, query + "\n" + "^")
        else:
            assert False, "Engine should raise exception if parsing fails"

    def test_preserve_whitespace(self):
        """ The engine should preserve the whitespace between fragments """
        query = "DUMP\nSCHEMA\n\n;"
        for fragment in query.split("\n"):
            self.query(fragment)
        self.assertEqual(self.engine.last_query, query)
