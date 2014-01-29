DQL - DynamoDB Query Language
=============================
A simple, SQL-ish language for DynamoDB

User Guide
----------

.. toctree::
    :maxdepth: 2
    :glob:

    topics/getting_started
    topics/advanced_usage
    topics/queries/index
    topics/develop

API Reference
-------------
.. toctree::
    :maxdepth: 3
    :glob:

    ref/dql

Versions
--------
=========  ===============  ========
Version    Build            Coverage
=========  ===============  ========
master_    |build-master|_  |coverage-master|_
=========  ===============  ========

.. _master: ../latest/
.. |build-master| image:: https://travis-ci.org/mathcamp/dql.png?branch=master
.. _build-master: https://travis-ci.org/mathcamp/dql
.. |coverage-master| image:: https://coveralls.io/repos/mathcamp/dql/badge.png?branch=master
.. _coverage-master: https://coveralls.io/r/mathcamp/dql?branch=master

Code lives here: https://github.com/mathcamp/dql

Changelog
---------

.. toctree::
    :maxdepth: 1
    :glob:

    changes

Indices and tables
------------------

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

TODO
----
* More unit test coverage
* Option to not truncate output
* Option to color indexed fields in output
