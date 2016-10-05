Developing
==========
To get started developing dql, run the following command::

    wget https://raw.github.com/stevearc/devbox/master/devbox/unbox.py && \
    python unbox.py git@github.com:stevearc/dql

This will clone the repository and install the package into a virtualenv

Running Tests
-------------
The command to run tests is ``python setup.py nosetests``. Some of these tests
require `DynamoDB Local
<http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Tools.html>`_.
There is a nose plugin that will download and run the DynamoDB Local service
during the tests. It requires the java 6/7 runtime, so make sure you have that
installed.
