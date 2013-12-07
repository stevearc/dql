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

**expression**
    Count only elements that match this expression. The supported operators are
    ``=``, ``!=``, ``>``, ``>=``, ``<``, and ``<=``. The only conjunction
    allowed is ``AND``.

    There is another form available that looks like ``KEYS IN (pkey) [,
    (pkey)]...`` The *pkey* is a single value if the table only has a hash
    key, or two comma-separated values if there is also a range key.

    When using the first form, DQL does a table query to find matching items
    and then performs the deletes. The second form does a batch get instead of
    a query.

**index**
    When the WHERE expression uses an indexed attribute, this allows you to
    manually specify which index name to use for the query. It should generally
    not be needed, as the DQL engine will automatically detect the correct
    index to use for a query.
