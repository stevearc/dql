""" Tests for model classes """
from dql.models import format_throughput

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
        desc = self.engine.describe("foobar", refresh=True)
        self.assertEqual(desc.total_read_throughput, 2)
        self.assertEqual(desc.total_write_throughput, 2)

    def test_format_throughput_for_available_throughput(self):
        """ Returns the properly formatted string for the throughput. """
        actual = format_throughput(20)
        self.assertEqual(actual, "20")

    def test_format_throughput_for_two_inputs(self):
        """ Returns the properly formatted string based on the inputs. """
        actual = format_throughput(20, 10)
        self.assertEqual(actual, "10/20 (50%)")

    def test_format_throughput_for_two_inputs_which_will_result_in_a_fraction(self):
        """ Returns the properly formatted string based on the inputs. """
        actual = format_throughput(20, 7)
        self.assertEqual(actual, "7/20 (35%)")

    def test_format_throughput_for_when_available_is_zero(self):
        """ Returns N/A as the available throughput is 0 """
        self.assertEqual(format_throughput(0, 7), "N/A")
        self.assertEqual(format_throughput(0), "N/A")

    def test_format_throughput_for_when_available_is_NaN(self):
        """ Returns N/A as the available throughput is NaN """
        self.assertEqual(format_throughput("asd", 7), "N/A")
        self.assertEqual(format_throughput("asd"), "N/A")
