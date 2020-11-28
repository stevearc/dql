import os
from pathlib import Path


class HistoryManager(object):
    _initial_history_length = 0

    def _create_dir_if_not_exists(self, path: str) -> None:
        if not os.path.exists(path):
            os.makedirs(path)

    def _create_file_if_nost_exists(self, path: str) -> None:
        if not os.path.exists(path):
            with open(path, "w"):
                pass

    def _get_history_dir_and_file(self):
        home = str(Path.home())
        history_dir = os.path.join(home, ".dql")
        self._create_dir_if_not_exists(history_dir)
        history_file = os.path.join(history_dir, "history")
        self._create_file_if_nost_exists(history_file)
        return (history_dir, history_file)

    def try_to_load_history(self):
        (history_dir, history_file) = self._get_history_dir_and_file()
        if os.path.exists(history_file):
            # print("History loading from file: " + history_file)
            try:
                import readline
            except ImportError:
                # Windows doesn't have readline, so gracefully ignore.
                pass
            else:
                readline.read_history_file(history_file)
                self._initial_history_length = readline.get_current_history_length()

    def try_to_write_history(self):
        (history_dir, history_file) = self._get_history_dir_and_file()
        try:
            import readline
        except ImportError:
            # Windows doesn't have readline, so gracefully ignore.
            pass
        else:
            current_history_length = readline.get_current_history_length()
            new_history_length = current_history_length - self._initial_history_length
            readline.append_history_file(new_history_length, history_file)
            print("History written to file: " + history_file)
