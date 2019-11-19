""" Tests for saving data to files """
import tempfile
import os

from . import BaseSystemTest

# pylint: disable=W0632


class TestSave(BaseSystemTest):

    """ System tests for saving to file """

    def _create_data(self):
        """ Create sample data in a test table """
        self.query("CREATE TABLE foobar (id STRING HASH KEY)")
        self.query("INSERT INTO foobar (id, foo) VALUES ('a', 1), ('b', 2)")

    def _save(self, filename):
        """ Query test table and save to a file """
        filepath = os.path.join(tempfile.gettempdir(), filename)
        self.query("SCAN * FROM foobar SAVE %s" % filepath)

    def test_pickle(self):
        """ Can save data as a pickle without crashing """
        self._create_data()
        self._save("out.p")

    def test_csv(self):
        """ Can save data as a csv without crashing """
        self._create_data()
        self._save("out.csv")

    def test_json(self):
        """ Can save data as json without crashing """
        self._create_data()
        self._save("out.json")

    def test_pickle_gz(self):
        """ Can save data as a gzipped pickle without crashing """
        self._create_data()
        self._save("out.p.gz")

    def test_csv_gz(self):
        """ Can save data as a gzipped csv without crashing """
        self._create_data()
        self._save("out.csv.gz")

    def test_json_gz(self):
        """ Can save data as gzipped json without crashing """
        self._create_data()
        self._save("out.json.gz")
