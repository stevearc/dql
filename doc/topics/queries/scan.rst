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
    SCAN foobars FILTER id = 'a' AND foo CONTAINS 4 LIMIT 100;
    SCAN foobars FILTER id = 'a' OR foo IS NULL;

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
FILTER clauses give the query a way to eliminate results server-side. The query
will still not be using an index, but it may be faster since it will transmit
less data. FILTER may use additional operators that WHERE cannot use (IS NULL,
IS NOT NULL, CONTAINS, and IN). FILTER may also join these conditions with AND
or OR (though not both).

Examples
########
.. code-block:: sql

    FILTER hkey = 'a' AND bar > 5 AND baz != 16
    FILTER bar BEGINS WITH "prefix"
    FILTER bar BETWEEN (1, 100)
    FILTER bar IS NULL AND baz IS NOT NULL
    FILTER bar CONTAINS 5 AND baz NOT CONTAINS 2
    FILTER bar BETWEEN (1, 100) OR baz BETWEEN (1, 100)
    FILTER bar IN (1, 3, 5, 7, 9)

Notes
#####
See the `AWS docs
<http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Scan.html>`_
for more information on scan parameters.
