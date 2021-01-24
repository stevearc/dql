import os
import readline
import shutil
import tempfile
from pathlib import Path
from unittest import TestCase

from dql.history import HistoryManager


class TestHistoryManager(TestCase):

    """ Tests for HistoryManager """

    historyManager: HistoryManager

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.historyManager = HistoryManager()

    def setUp(self):
        super().setUp()
        self._histDir = tempfile.mkdtemp()
        self._histFile = os.path.join(self._histDir, HistoryManager.history_file_name)
        readline.clear_history()

    def tearDown(self):
        super().tearDown()
        shutil.rmtree(self._histDir)

    def assertFileExists(self, file_path):
        self.assertTrue(os.path.isfile(file_path))

    def assertFileContents(self, file_path, expected_contents):
        actual = Path(file_path).read_text()
        self.assertEqual(expected_contents, actual)

    def test_history_file_is_created_on_load(self):
        """ Assert that a history file is created in the history directory on load """
        expectedHistFilePath = self._histFile

        self.historyManager.try_to_load_history(self._histDir)
        self.assertFileExists(expectedHistFilePath)

    def test_history_file_is_created_on_write(self):
        """ Assert that a history file is created in the history directory on write """
        expectedHistFilePath = self._histFile

        self.historyManager.try_to_write_history(self._histDir)
        self.assertFileExists(expectedHistFilePath)

    def test_history_file_contains_history_from_readline(self):
        """ Assert that a history file will be written with proper contents. """
        expectedHistFilePath = self._histFile

        readline.add_history("this is a simulated cli input")
        self.historyManager.try_to_write_history(self._histDir)
        self.assertFileExists(expectedHistFilePath)
        self.assertFileContents(expectedHistFilePath, "this is a simulated cli input\n")

    def test_history_file_contains_proper_appended_history(self):
        """ Assert that a history file will be appended to """
        expectedHistFilePath = self._histFile

        readline.add_history("this is a simulated cli input")
        self.historyManager.try_to_write_history(self._histDir)
        self.historyManager.try_to_load_history(self._histDir)
        readline.add_history("another simulated cli input")
        self.historyManager.try_to_write_history(self._histDir)

        self.assertFileExists(expectedHistFilePath)
        self.assertFileContents(
            expectedHistFilePath,
            "this is a simulated cli input\nanother simulated cli input\n",
        )
