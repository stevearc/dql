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

Pre-requisites

- Install `pyenv <https://github.com/pyenv/pyenv>`_
    - Why use pyenv? `Intro to pyenv <https://realpython.com/intro-to-pyenv/#what-about-a-package-manager>`_
- Install `pyenv-virtualenv <https://github.com/pyenv/pyenv-virtualenv#installing-with-homebrew-for-macos-users>`_ so that you can manage virtualenvs from pyenv.
- Install Java: I recomend using `sdkman <https://sdkman.io/install>`_ to manage your java installations.
    - I use java version 8.0.265.j9-adpt
    - ``sdk install java 8.0.265.j9-adpt``

Setting up local envs::

    # See installed python versions
    pyenv versions

    # See which python you are currently using. This will also show missing versions
    # required by .python-version file.
    pyenv which python

    # Install the required python versions using pyenv.
    pyenv install <version-number>

    # Create a virtual env named "dql-local-env" with python version 3.7.7
    pyenv virtualenv 3.7.7 dql-local-env

    # Look at the virtual envs. dql-local-env should have a * next to it indicating
    # that it is selected.
    pyenv virtualenvs

    # You should be currently using "~/.pyenv/versions/dql-local-env/bin/python"
    pyenv which python

    # install dependencies
    pip install -r requirements_dev.txt


Versioning
----------
Use `bump2version` instead of `bumpversion` because `bump2version` is actively maintained. This advise comes from `bumpversion` project itself. See `bumpversion`'s pypi page for details.

Config based on: `<https://medium.com/@williamhayes/versioning-using-bumpversion-4d13c914e9b8>`_

Usage::

    # will update the relevant part and start a new `x.x.x-dev0` build version
    $> bump2version patch
    $> bump2version minor
    $> bump2version major

    # update the build number from `x.x.x-dev0` to `x.x.x-dev1`
    $> bump2version build

    # release when ready, will convert the version to `x.x.x`, commit and tag it.
    $> bump2version --tag release

