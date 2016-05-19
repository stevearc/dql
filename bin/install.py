#!/usr/bin/env python
""" Script for building a standalone dql executable """
import os
import tempfile
import sys
import shutil
import subprocess
from distutils.spawn import find_executable  # pylint: disable=E0611,F0401

# Python 2 & 3
try:
    from urllib import urlretrieve
except ImportError:
    from urllib.request import urlretrieve  # pylint: disable=E0611,F0401

VENV_VERSION = '15.0.1'
VENV_URL = ("https://pypi.python.org/packages/source/v/"
            "virtualenv/virtualenv-%s.tar.gz" % VENV_VERSION)


def make_virtualenv(env):
    """ Create a virtualenv """
    if find_executable('virtualenv') is not None:
        cmd = ['virtualenv'] + [env]
        subprocess.check_call(cmd)
    else:
        # Otherwise, download virtualenv from pypi
        path = urlretrieve(VENV_URL)[0]
        subprocess.check_call(['tar', 'xzf', path])
        subprocess.check_call(
            [sys.executable, "virtualenv-%s/virtualenv.py" % VENV_VERSION,
             env])
        os.unlink(path)
        shutil.rmtree("virtualenv-%s" % VENV_VERSION)


def main():
    """ Build a standalone dql executable """
    venv_dir = tempfile.mkdtemp()
    try:
        make_virtualenv(venv_dir)

        print("Downloading dependencies")
        pip = os.path.join(venv_dir, 'bin', 'pip')
        subprocess.check_call([pip, 'install', 'pex'])

        print("Building executable")
        pex = os.path.join(venv_dir, 'bin', 'pex')
        subprocess.check_call([pex, 'dql', '-m', 'dql:main', '-o', 'dql'])

        print("dql executable written to %s" % os.path.abspath('dql'))
    finally:
        shutil.rmtree(venv_dir)


if __name__ == '__main__':
    main()
