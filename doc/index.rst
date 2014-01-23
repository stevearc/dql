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
* Math expressions in UPDATE
* Subqueries
* Auto-load modules into scope
* Arbitrary function support (e.g. now())
* Create indexes with different projection mappings
* More unit test coverage
* THROTTLE keyword to avoid breaking throughput
