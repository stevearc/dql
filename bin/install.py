#!/usr/bin/env python
""" Script for building a standalone dql executable """
import os
import shutil
import subprocess
import sys
import tempfile
from distutils.spawn import find_executable
from urllib.request import urlretrieve


def make_virtualenv(env):
    """ Create a virtualenv """
    if find_executable("virtualenv") is not None:
        cmd = ["virtualenv", env]
    else:
        cmd = ["python", "-m", "venv", env]
    subprocess.check_call(cmd)


def main():
    """ Build a standalone dql executable """
    venv_dir = tempfile.mkdtemp()
    try:
        make_virtualenv(venv_dir)

        print("Downloading dependencies")
        pip = os.path.join(venv_dir, "bin", "pip")
        subprocess.check_call([pip, "install", "pex"])

        print("Building executable")
        pex = os.path.join(venv_dir, "bin", "pex")
        subprocess.check_call([pex, "dql", "-m", "dql:main", "-o", "dql"])

        print("dql executable written to %s" % os.path.abspath("dql"))
    finally:
        shutil.rmtree(venv_dir)


if __name__ == "__main__":
    main()
