""" Tests for saving data to files """
import os
import shutil
import tempfile

from . import BaseSystemTest

# pylint: disable=W0632


class TestSave(BaseSystemTest):

    """ System tests for saving to file """

    def setUp(self):
        super().setUp()
        self.tmpdir = tempfile.mkdtemp()
        self.query("CREATE TABLE foobar (id STRING HASH KEY)")
        self.query("INSERT INTO foobar (id, foo) VALUES ('a', 1), ('b', 2)")
        self.query("CREATE TABLE destination (id STRING HASH KEY)")

    def tearDown(self):
        super().tearDown()
        shutil.rmtree(self.tmpdir)

    def _save(self, filename):
        """ Query test table and save to a file """
        filepath = os.path.join(self.tmpdir, filename)
        self.query("SCAN * FROM foobar SAVE %s" % filepath)
        return filepath

    def test_file_formats(self):
        """ Test saving and loading all file formats """
        formats = ["p", "csv", "json", "p.gz", "csv.gz", "json.gz"]
        for fmt in formats:
            filename = self._save("out.%s" % fmt)
            self.query("LOAD %s INTO destination" % filename)
            res1 = list(self.query("SCAN * FROM foobar"))
            res2 = list(self.query("SCAN * FROM destination"))
            self.assertCountEqual(res2, res1)
            self.query("DELETE FROM destination")
