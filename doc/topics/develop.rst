Developing
==========
To get started developing dql, clone the repo::

    git clone https://github.com/stevearc/dql.git

It is recommended that you create a virtualenv to develop::

    # python 3
    python3 -m venv dql_env
    # python 2
    virtualenv dql_env

    source ./dql_env/bin/activate
    pip install -e .

Running Tests
-------------
The command to run tests is ``python setup.py nosetests``, but I recommend using
`tox <https://tox.readthedocs.io/en/latest/>`__. Some of these tests require
`DynamoDB Local
<http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Tools.html>`_.
There is a nose plugin that will download and run the DynamoDB Local service
during the tests. It requires the java 6/7 runtime, so make sure you have that
installed.


Local dev Using ``pyenv pyenv-virtualenv tox tox-pyenv``
--------------------------------------------------------

Pre-requisites:

- `pyenv <https://github.com/pyenv/pyenv>`__
    - `Intro to pyenv <https://realpython.com/intro-to-pyenv/#what-about-a-package-manager>`__
- `pyenv-virtualenv <https://github.com/pyenv/pyenv-virtualenv#installing-with-homebrew-for-macos-users>`__


Setting up local envs::

    # see installed python versions
    pyenv versions

    # see which python you are currently using. This will also show missing versions required by .python-version file.
    pyenv which python

    # Install the required python versions using pyenv.
    pyenv install <version-number>

    # create a virtual env named "dql-local-env" with python version 3.7.7
    pyenv virtualenv 3.7.7 dql-local-env

    # look at the virtual envs. dql-local-env should have a * next to it indicating that it is selected.
    pyenv virtualenvs

    # you should be currently using "~/.pyenv/versions/dql-local-env/bin/python"
    pyenv which python


    # install dependencies
    pip install -r requirements_dev.txt
