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
