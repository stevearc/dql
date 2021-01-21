import os
import readline
import shutil
import tempfile
from pathlib import Path
from unittest import TestCase

from dql.history import HistoryManager


class BaseHistoryManagerTest(TestCase):

    """ Base class for HistoryManager tests """

    historyManager: HistoryManager

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.historyManager = HistoryManager()
        readline.clear_history()

    @classmethod
    def tearDownClass(cls):
        super().tearDownClass()

    def get_hist_file_path(self, hist_dir):
        return os.path.join(hist_dir, "history")

    def setUp(self):
        super().setUp()
        readline.clear_history()


class TestHistoryManager(BaseHistoryManagerTest):

    """ Tests for HistoryManager """

    def assertFileExists(self, file_path):
        self.assertTrue(os.path.isfile(file_path))

    def assertFileContents(self, file_path, expected_contents):
        actual = Path(file_path).read_text()
        self.assertEqual(expected_contents, actual)

    def test_history_file_is_created_on_load(self):
        """ Assert that a history file is created in the history directory on load """
        histDir = tempfile.mkdtemp()
        expectedHistFilePath = self.get_hist_file_path(histDir)

        self.historyManager.try_to_load_history(histDir)
        self.assertFileExists(expectedHistFilePath)

        shutil.rmtree(histDir)

    def test_history_file_is_created_on_write(self):
        """ Assert that a history file is created in the history directory on write """
        histDir = tempfile.mkdtemp()
        expectedHistFilePath = self.get_hist_file_path(histDir)

        self.historyManager.try_to_write_history(histDir)
        self.assertFileExists(expectedHistFilePath)

        shutil.rmtree(histDir)

    def test_history_file_contains_history_from_readline(self):
        """ Assert that a history file will be written with proper contents. """
        histDir = tempfile.mkdtemp()
        expectedHistFilePath = self.get_hist_file_path(histDir)

        readline.add_history("this is a simulated cli input")
        self.historyManager.try_to_write_history(histDir)
        self.assertFileExists(expectedHistFilePath)
        self.assertFileContents(expectedHistFilePath, "this is a simulated cli input\n")

        shutil.rmtree(histDir)

    def test_history_file_contains_proper_appended_history(self):
        """ Assert that a history file will be appended to """
        histDir = tempfile.mkdtemp()
        expectedHistFilePath = self.get_hist_file_path(histDir)

        readline.add_history("this is a simulated cli input")
        self.historyManager.try_to_write_history(histDir)
        self.historyManager.try_to_load_history(histDir)
        readline.add_history("another simulated cli input")
        self.historyManager.try_to_write_history(histDir)

        self.assertFileExists(expectedHistFilePath)
        self.assertFileContents(
            expectedHistFilePath,
            "this is a simulated cli input\nanother simulated cli input\n",
        )

        shutil.rmtree(histDir)
