import os
from pathlib import Path
from typing import Optional


class HistoryManager(object):
    _initial_history_length = 0
    history_file_name = "history"

    def _create_file_if_not_exists(self, path: str) -> None:
        with open(path, "a"):
            pass

    def _prep_history_file(self, history_dir: Optional[str] = None) -> str:
        """
        Will return the path to the prepared hisory file.
        Prepping involves creating the dirs and the file itself.

        Returns
        -------
            str The path to the history file.
        """
        default_history_dir = os.path.join(Path.home(), ".dql")
        actual_history_dir = history_dir or default_history_dir
        os.makedirs(actual_history_dir, exist_ok=True)
        history_file = os.path.join(actual_history_dir, self.history_file_name)
        self._create_file_if_not_exists(history_file)
        return history_file

    def try_to_load_history(self, history_dir: Optional[str] = None) -> None:
        history_file = self._prep_history_file(history_dir)
        try:
            import readline
        except ImportError:
            # Windows doesn't have readline, so gracefully ignore.
            pass
        else:
            readline.read_history_file(history_file)
            self._initial_history_length = readline.get_current_history_length()

    def try_to_write_history(self, history_dir: Optional[str] = None) -> None:
        history_file = self._prep_history_file(history_dir)
        try:
            import readline
        except ImportError:
            # Windows doesn't have readline, so gracefully ignore.
            pass
        else:
            current_history_length = readline.get_current_history_length()
            new_history_length = current_history_length - self._initial_history_length
            if new_history_length < 0:
                raise Exception(
                    f"Unable to write new history. Length is less than 0. ({current_history_length} - {self._initial_history_length})"
                )
            else:
                # append will fail if the file does not exist.
                readline.append_history_file(new_history_length, history_file)
