SELECT
======

Synopsis
--------
.. code-block:: sql

    SELECT
        [ CONSISTENT ]
        attributes
        FROM tablename
        WHERE expression
        [ FILTER expression ]
        [ USING index ]
        [ LIMIT limit ]
        [ ASC | DESC ]

Examples
--------
.. code-block:: sql

    SELECT * FROM foobars WHERE foo = 'bar';
    SELECT CONSISTENT * foobars WHERE foo = 'bar' AND baz >= 3;
    SELECT * foobars WHERE foo = 'bar' AND baz > 3 FILTER data IS NOT NULL;
    SELECT foo, bar FROM foobars WHERE id = 'a' AND ts < 100 USING 'ts-index';
    SELECT * FROM foobars WHERE foo = 'bar' AND baz >= 3 LIMIT 50 DESC;

Description
-----------
Query a table for items.

Parameters
----------
**CONSISTENT**
    If this is present, use a read-consistent query

**attributes**
    Comma-separated list of item attributes to fetch. ``*`` is a special case
    meaning 'all attributes'.

**tablename**
    The name of the table

**index**
    When the WHERE expression uses an indexed attribute, this allows you to
    manually specify which index name to use for the query. It should generally
    not be needed, as the DQL engine will automatically detect the correct
    index to use for a query.

**limit**
    Maximum number of results to return

**ASC | DESC**
    Sort the results in ASCending (the default) or DESCending order.

Where Clause
------------
This must be present in any SELECT statement, as it allows the query to use an
index. Queries with no index are done with a SCAN. Only indexed fields may be
used in a WHERE clause.

Examples
########
.. code-block:: sql

    WHERE hkey = 'a' AND bar > 5 AND baz <= 16
    WHERE hkey = 1 AND bar BEGINS WITH "prefix"
    WHERE hkey = 1 AND bar BETWEEN (1, 100)
    WHERE KEYS IN ('hkey1', 'rkey1'), ('hkey2', 'rkey2')
    WHERE KEYS IN ('hkey'), ('hkey2')

Notes
#####
When using the ``KEYS IN`` form, DQL will perform a batch get instead of a
table query. See the `AWS docs
<http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Query.html>`_
for more information on query parameters.

Filter Clause
-------------
FILTER clauses give the query a way to eliminate results server-side. After the query
has found the indexed results, it will apply the filter before returning them.
FILTER may use additional operators that WHERE cannot use (IS NULL, IS NOT
NULL, CONTAINS, and IN). FILTER may also join these conditions with AND or OR
(though not both).

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
