""" Testing tools for DQL """
from dql import Engine


try:
    import unittest2 as unittest  # pylint: disable=F0401
except ImportError:
    import unittest


if not hasattr(unittest.TestCase, 'assertItemsEqual'):
    unittest.TestCase.assertItemsEqual = unittest.TestCase.assertCountEqual


class BaseSystemTest(unittest.TestCase):

    """ Base class for system tests """
    dynamo = None

    def setUp(self):
        super(BaseSystemTest, self).setUp()
        self.engine = Engine(self.dynamo)
        # Clear out any pre-existing tables
        for tablename in self.dynamo.list_tables():
            self.dynamo.delete_table(tablename)

    def tearDown(self):
        super(BaseSystemTest, self).tearDown()
        for tablename in self.dynamo.list_tables():
            self.dynamo.delete_table(tablename)

    def query(self, command):
        """ Shorthand because I'm lazy """
        return self.engine.execute(command)

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
        return name
