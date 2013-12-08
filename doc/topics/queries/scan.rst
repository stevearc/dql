SCAN
====

Synopsis
--------
.. code-block:: sql

    SCAN tablename
        [ FILTER expression ]
        [ LIMIT limit ]

Examples
--------
.. code-block:: sql

    SCAN foobars;
    SCAN foobars FILTER id = 'a' AND foo = 4;
    SCAN foobars FILTER (id = 'a' AND foo = 4) LIMIT 100;

Description
-----------
Sequentially iterate over items in a table. This does not use the indexes, so
it can be significantly slower than a query.

Parameters
----------
**tablename**
    The name of the table

**limit**
    Maximum number of results to return

Filter Clause
-------------

Examples
########
.. code-block:: sql

    FILTER hkey = 'a' AND bar > 5 AND baz != 16
    FILTER hkey = 1 AND bar BEGINS WITH "prefix"
    FILTER hkey = 1 AND bar BETWEEN (1, 100)
    FILTER hkey = 1 AND bar IS NULL AND baz IS NOT NULL
    FILTER hkey = 1 AND bar CONTAINS 5 AND baz NOT CONTAINS 5

Notes
#####
See the `AWS docs
<http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Scan.html>`_
for more information on scan parameters.
