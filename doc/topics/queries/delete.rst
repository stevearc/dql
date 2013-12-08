DELETE
======

Synopsis
--------
.. code-block:: sql

    DELETE FROM
        tablename
        WHERE expression
        [ USING index ]

Examples
--------
.. code-block:: sql

    DELETE FROM foobars WHERE foo = 'bar';
    DELETE FROM foobars WHERE foo != 'bar' AND baz >= 3;
    DELETE FROM foobars WHERE KEYS IN ('hkey1'), ('hkey2');
    DELETE FROM foobars WHERE KEYS IN ('hkey1', 'rkey1'), ('hkey2', 'rkey2');
    DELETE FROM foobars WHERE (foo = 'bar' AND baz >= 3) USING 'baz-index';

Description
-----------

Parameters
----------
**tablename**
    The name of the table

**index**
    When the WHERE expression uses an indexed attribute, this allows you to
    manually specify which index name to use for the query. It should generally
    not be needed, as the DQL engine will automatically detect the correct
    index to use for a query.

Where Clause
------------

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
