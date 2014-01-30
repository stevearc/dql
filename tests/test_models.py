""" Tests for model classes """
from . import BaseSystemTest


class TestModels(BaseSystemTest):
    """ Tests for model classes """

    def test_total_throughput(self):
        """ Total table throughput sums table and global indexes """
        self.query(
            "CREATE TABLE foobar "
            "(id STRING HASH KEY, foo NUMBER, THROUGHPUT (1, 1))"
            "GLOBAL INDEX ('idx', id, foo, THROUGHPUT(1, 1))"
        )
        desc = self.engine.describe('foobar', refresh=True)
        self.assertEquals(desc.total_read_throughput, 2)
        self.assertEquals(desc.total_write_throughput, 2)
