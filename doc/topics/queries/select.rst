.. _select:

SELECT
======

Synopsis
--------
.. code-block:: sql

    SELECT
        [ CONSISTENT ]
        attributes
        FROM tablename
        [ KEYS IN primary_keys | WHERE expression ]
        [ USING index ]
        [ LIMIT limit ]
        [ SCAN LIMIT scan_limit ]
        [ ORDER BY field ]
        [ ASC | DESC ]
        [ THROTTLE throughput ]
        [ SAVE filename]

Examples
--------
.. code-block:: sql

    SELECT * FROM foobars SAVE out.p;
    SELECT * FROM foobars WHERE foo = 'bar';
    SELECT count(*) FROM foobars WHERE foo = 'bar';
    SELECT id, TIMESTAMP(updated) FROM foobars KEYS IN 'id1', 'id2';
    SELECT * FROM foobars KEYS IN ('hkey', 'rkey1'), ('hkey', 'rkey2');
    SELECT CONSISTENT * foobars WHERE foo = 'bar' AND baz >= 3;
    SELECT * foobars WHERE foo = 'bar' AND attribute_exists(baz);
    SELECT * foobars WHERE foo = 1 AND NOT (attribute_exists(bar) OR contains(baz, 'qux'));
    SELECT 10 * (foo - bar) FROM foobars WHERE id = 'a' AND ts < 100 USING 'ts-index';
    SELECT * FROM foobars WHERE foo = 'bar' LIMIT 50 DESC;
    SELECT * FROM foobars THROTTLE (50%, *);

Description
-----------
Query a table for items.

Parameters
----------
**CONSISTENT**
    If this is present, perform a strongly consistent read

**attributes**
    Comma-separated list of attributes to fetch or expressions. You can use the
    ``TIMESTAMP`` and ``DATE`` functions, as well as performing simple,
    arbitrarily nested arithmetic (``foo + (bar - 3) / 100``). ``SELECT *`` is a
    special case meaning 'all attributes'. ``SELECT count(*)`` is a special case
    that will return the number of results, rather than the results themselves.

**tablename**
    The name of the table

**index**
    When the WHERE expression uses an indexed attribute, this allows you to
    manually specify which index name to use for the query. You will only need
    this if the constraints provided match more than one index.

**limit**
    The maximum number of items to return.

**scan_limit**
    The maximum number of items for DynamoDB to scan (not necessarily the number
    of matching items returned).

**ORDER BY**
    Sort the results by a field.

.. warning::

    Using ORDER BY with LIMIT may produce unexpected results. If you use ORDER
    BY on the range key of the index you are querying on, it will work as
    expected. Otherwise, DQL will fetch the number of results specified by the
    LIMIT and then sort them.

**ASC | DESC**
    Sort the results in ASCending (the default) or DESCending order.

**THROTTLE**
    Limit the amount of throughput this query can consume. This is a pair of
    values for ``(read_throughput, write_throughput)``. You can use a flat
    number or a percentage (e.g. ``20`` or ``50%``). Using ``*`` means no limit
    (typically useless unless you have set a default throttle in the
    :ref:`options`).

**SAVE**
    Save the results to a file. By default the items will be encoded with
    pickle, but the '.json' and '.csv' extensions will use the proper format.
    You may also append a '.gz' or '.gzip' afterwards to gzip the results. Note
    that the JSON and CSV formats will be lossy because they cannot properly
    encode some data structures, such as sets.

Where Clause
------------
If provided, the SELECT operation will use these constraints as the
``KeyConditionExpression`` if possible, and if not (or if there are constraints
left over), the ``FilterExpression``.  All query syntax is pulled directly from
the AWS docs:
http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/QueryAndScan.html

In general, you may use any syntax mentioned in the docs, but you don't need to
worry about reserved words or passing in data as variables like ``:var1``. DQL
will handle that for you.

Notes
#####
When using the ``KEYS IN`` form, DQL will perform a batch get instead of a
table query. See the `AWS docs
<http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Query.html>`_
for more information on query parameters.
