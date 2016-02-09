UPDATE
======

Synopsis
--------
.. code-block:: sql

    UPDATE tablename
        update_expression
        [ KEYS IN primary_keys ]
        [ WHERE expression ]
        [ USING index ]
        [ RETURNS (NONE | ( ALL | UPDATED) (NEW | OLD)) ]
        [ THROTTLE throughput ]

Examples
--------
.. code-block:: sql

    UPDATE foobars SET foo = 'a';
    UPDATE foobars SET foo = 'a', bar = bar + 4 WHERE id = 1 AND foo = 'b';
    UPDATE foobars SET foo = if_not_exists(foo, 'a') RETURNS ALL NEW;
    UPDATE foobars SET foo = list_append(foo, 'a') WHERE size(foo) < 3;
    UPDATE foobars ADD foo 1, bar 4;
    UPDATE foobars ADD fooset (1, 2);
    UPDATE foobars REMOVE old_attribute;
    UPDATE foobars DELETE fooset (1, 2);

Description
-----------
Update items in a table

Parameters
----------
**tablename**
    The name of the table

**RETURNS**
    Return the items that were operated on. Default is RETURNS NONE. See the
    Amazon docs for `UpdateItem
    <http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_UpdateItem.html>`_
    for more detail.

**THROTTLE**
    Limit the amount of throughput this query can consume. This is a pair of
    values for ``(read_throughput, write_throughput)``. You can use a flat
    number or a percentage (e.g. ``20`` or ``50%``). Using ``*`` means no limit
    (typically useless unless you have set a default throttle in the
    :ref:`options`).

Update expression
-----------------
All update syntax is pulled directly from the AWS docs:

http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.Modifying.html

In general, you may use any syntax mentioned in the docs, but you don't need to
worry about reserved words or passing in data as variables like ``:var1``. DQL
will handle that for you.

WHERE and KEYS IN
-----------------
Both of these expressions are the same as in :ref:`select`. Note that using
``KEYS IN`` is more efficient because DQL can perform the writes without doing a
query first.
