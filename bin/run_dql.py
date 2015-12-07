#!/usr/bin/env python
"""
Standalone script that will download & run DQL in a virtualenv.

Created from https://github.com/stevearc/python-bootstrap

"""
import os
import shutil
import subprocess
import sys
import tempfile
from distutils.spawn import find_executable  # pylint: disable=E0611,F0401


# Python 2 & 3
try:
    from urllib import urlretrieve
except ImportError:
    from urllib.request import urlretrieve  # pylint: disable=E0611,F0401

VENV_VERSION = '13.1.2'
VENV_URL = ("https://pypi.python.org/packages/source/v/"
            "virtualenv/virtualenv-%s.tar.gz" % VENV_VERSION)
VENV_NAME = 'dql_env'
VERSION = '0.5.9'


def bootstrap_virtualenv(env):
    """
    Activate a virtualenv, creating it if necessary.
    Parameters
    ----------
    env : str
        Path to the virtualenv
    """
    if not os.path.exists(env):
        # If virtualenv command exists, use that
        if find_executable('virtualenv') is not None:
            cmd = ['virtualenv'] + [env]
            subprocess.check_call(cmd)
        else:
            # Otherwise, download virtualenv from pypi
            path = urlretrieve(VENV_URL)[0]
            subprocess.check_call(['tar', 'xzf', path])
            subprocess.check_call(
                [sys.executable,
                 "virtualenv-%s/virtualenv.py" % VENV_VERSION,
                 env])
            os.unlink(path)
            shutil.rmtree("virtualenv-%s" % VENV_VERSION)
        print("Created virtualenv %s" % env)
    restart(env)


def restart(env):
    """ Restart into a virtualenv """
    executable = os.path.join(env, 'bin', 'python')
    os.execv(executable, [executable] + sys.argv)


def is_inside_virtualenv(env):
    """ Check if running inside the virtualenv """
    return any((p.startswith(env) for p in sys.path))


def main():
    """ Main method """
    venv_dir = os.path.join(tempfile.gettempdir(), VENV_NAME)
    if not is_inside_virtualenv(venv_dir):
        bootstrap_virtualenv(venv_dir)
        return
    try:
        __import__('dql')
        import dql
        need_install = VERSION != dql.__version__
    except ImportError:
        need_install = True
    if need_install:
        pip = os.path.join(venv_dir, 'bin', 'pip')
        subprocess.check_call([pip, 'install', "dql==%s" % VERSION])
        # If we installed a new version, we need to restart
        if 'dql' in sys.modules:
            restart(venv_dir)

    import dql
    dql.main()


if __name__ == '__main__':
    main()
