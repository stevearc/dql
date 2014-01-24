UPDATE
======

Synopsis
--------
.. code-block:: sql

    UPDATE tablename
        SET values
        [ WHERE expression ]
        [ RETURNS (NONE | ( ALL | UPDATED) (NEW | OLD)) ]

Examples
--------
.. code-block:: sql

    UPDATE foobars SET foo = 'a';
    UPDATE foobars SET foo = 'a', bar += 4 WHERE id = 1 AND foo = 'b';
    UPDATE foobars SET foo = 'a', bar += 4 RETURNS ALL NEW;
    UPDATE foobars SET myset << (5, 6, 7), mytags << 'new tag' WHERE KEYS IN ('a', 'b');
    UPDATE foobars SET foo = `bar + 1`;

Description
-----------
Update the attributes on items in your table.

Parameters
----------
**tablename**
    The name of the table

**values**
    Comma-separated list of attribute assignments. The supported operators are
    ``=``, ``+=``, ``-=``, ``<<``, and ``>>``. The 'shovel' operators (``<<`` &
    ``>>``) are used to atomically add/remove items to/from a set. Likewise,
    the ``+=`` and ``-=`` perform atomic inc/decrement and may only be used on
    NUMBER types.

**expression**
    Only return elements that match this expression. The supported operators
    are ``=``, ``!=``, ``>``, ``>=``, ``<``, and ``<=``. The only conjunction
    allowed is ``AND``.

    There is another form available that looks like ``KEYS IN (pkey) [,
    (pkey)]...`` The *pkey* is a single value if the table only has a hash
    key, or two comma-separated values if there is also a range key.

    When using the first form, DQL does a table query to find matching items
    and then performs the updates. The second form performs the updates with no
    reads necessary. If no expression is specified, DQL will perform a
    full-table scan.

**RETURNS**
    Return the items that were operated on. Default is RETURNS NONE. See the
    Amazon docs for `UpdateItem
    <http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_UpdateItem.html>`_
    for more detail.

Notes
-----
When using python expressions to set values, you may reference attributes on
the table row:

.. code-block:: sql

    UPDATE foobars SET foo = `bar + 1`

If you aren't sure if the attribute will exist or not, you can reference the
row dict directly:

.. code-block:: sql

    us-west-1> UPDATE foobars SET foo = m`if row.get('bar'):
             >     return bar + 1
             > else:
             >     return 1`;

This syntax will NOT WORK if you are using the ``KEYS IN`` form of the query,
as that performs the update without doing any table reads.
