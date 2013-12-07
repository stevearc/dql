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
        [ USING index ]
        [ LIMIT limit ]
        [ ASC | DESC ]

Examples
--------
.. code-block:: sql

    SELECT * FROM foobars WHERE foo = 'bar'
    SELECT CONSISTENT * foobars WHERE foo != 'bar' AND baz >= 3
    SELECT foo, bar FROM foobars WHERE (id = 'a' AND ts < 100) USING 'ts-index'
    SELECT * FROM foobars WHERE foo = 'bar' AND baz >= 3 LIMIT 50 DESC

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

**expression**
    Count only elements that match this expression. The supported operators are
    ``=``, ``!=``, ``>``, ``>=``, ``<``, and ``<=``. The only conjunction
    allowed is ``AND``.

    There is another form available that looks like ``KEYS IN (pkey) [,
    (pkey)]...`` The *pkey* is a single value if the table only has a hash
    key, or two comma-separated values if there is also a range key.

    When using the first form, DQL does a table query. The second form does a
    batch get.

**index**
    When the WHERE expression uses an indexed attribute, this allows you to
    manually specify which index name to use for the query. It should generally
    not be needed, as the DQL engine will automatically detect the correct
    index to use for a query.

**limit**
    Maximum number of results to return

**ASC | DESC**
    Sort the results in ASCending (the default) or DESCending order.
