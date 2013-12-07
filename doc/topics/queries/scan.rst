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

    SCAN foobars
    SCAN foobars FILTER id = 'a' AND foo = 4
    SCAN foobars FILTER (id = 'a' AND foo = 4) LIMIT 100

Description
-----------
Sequentially iterate over items in a table. This does not use the indexes, so
it can be significantly slower than a query.

Parameters
----------
**tablename**
    The name of the table

**expression**
    Only return elements that match this expression. The supported operators
    are ``=``, ``!=``, ``>``, ``>=``, ``<``, and ``<=``. The only conjunction
    allowed is ``AND``.

**limit**
    Maximum number of results to return
