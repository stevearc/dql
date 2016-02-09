DELETE
======

Synopsis
--------
.. code-block:: sql

    DELETE FROM
        tablename
        [ KEYS IN primary_keys ]
        [ WHERE expression ]
        [ USING index ]
        [ THROTTLE throughput ]

Examples
--------
.. code-block:: sql

    DELETE FROM foobars; -- This will delete all items in the table!
    DELETE FROM foobars WHERE foo != 'bar' AND baz >= 3;
    DELETE FROM foobars KEYS IN 'hkey1', 'hkey2' WHERE attribute_exists(foo);
    DELETE FROM foobars KEYS IN ('hkey1', 'rkey1'), ('hkey2', 'rkey2');
    DELETE FROM foobars WHERE (foo = 'bar' AND baz >= 3) USING 'baz-index';

Description
-----------

Parameters
----------
**tablename**
    The name of the table

**primary_keys**
    List of the primary keys of the items to delete

**expression**
    See :ref:`select` for details about the WHERE clause

**index**
    When the WHERE expression uses an indexed attribute, this allows you to
    manually specify which index name to use for the query. You will only need
    this if the constraints provided match more than one index.

**THROTTLE**
    Limit the amount of throughput this query can consume. This is a pair of
    values for ``(read_throughput, write_throughput)``. You can use a flat
    number or a percentage (e.g. ``20`` or ``50%``). Using ``*`` means no limit
    (typically useless unless you have set a default throttle in the
    :ref:`options`).

Notes
#####
Using the ``KEYS IN`` form is much more efficient because DQL will not have to
perform a query first to get the primary keys.
