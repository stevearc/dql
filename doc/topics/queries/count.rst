COUNT
=====

Synopsis
--------
.. code-block:: sql

    COUNT
        [ CONSISTENT ]
        tablename
        WHERE expression
        [ USING index ]

Examples
--------
.. code-block:: sql

    COUNT foobars WHERE foo = 'bar';
    COUNT CONSISTENT foobars WHERE foo != 'bar' AND baz >= 3;
    COUNT foobars WHERE (foo = 'bar' AND baz >= 3) USING 'baz-index';

Description
-----------
This counts the number of matching items in your table. It is making a query,
so you *must* search using the hash key, and you may optionally also provide
the range key or an index.

The WHERE clause is mandatory. If you want a count of all elements in a table,
look at the table description.

Parameters
----------
**CONSISTENT**
    If this is present, use a read-consistent query

**tablename**
    The name of the table

**expression**
    Count only elements that match this expression. The supported operators are
    ``=``, ``!=``, ``>``, ``>=``, ``<``, and ``<=``. The only conjunction
    allowed is ``AND``.

**index**
    When the WHERE expression uses an indexed attribute, this allows you to
    manually specify which index name to use for the query. It should generally
    not be needed, as the DQL engine will automatically detect the correct
    index to use for a query.
