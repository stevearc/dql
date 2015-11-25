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
        [ ORDER BY field ]
        [ ASC | DESC ]
        [ SAVE file.json ]

Examples
--------
.. code-block:: sql

    SELECT * FROM foobars SAVE out.json;
    SELECT * FROM foobars WHERE foo = 'bar';
    SELECT count(*) FROM foobars WHERE foo = 'bar';
    SELECT * FROM foobars KEYS IN 'id1', 'id2';
    SELECT * FROM foobars KEYS IN ('hkey', 'rkey1'), ('hkey', 'rkey2');
    SELECT CONSISTENT * foobars WHERE foo = 'bar' AND baz >= 3;
    SELECT * foobars WHERE foo = 'bar' AND attribute_exists(baz);
    SELECT * foobars WHERE foo = 1 AND NOT (attribute_exists(bar) OR attribute_exists(baz));
    SELECT foo, bar FROM foobars WHERE id = 'a' AND ts < 100 USING 'ts-index';
    SELECT * FROM foobars WHERE foo = 'bar' LIMIT 50 DESC;

Description
-----------
Query a table for items.

Parameters
----------
**CONSISTENT**
    If this is present, perform a strongly consistent read

**attributes**
    Comma-separated list of item attributes to fetch. ``*`` is a special case
    meaning 'all attributes'. ``count(*)`` is a special case that will return
    the number of results, rather than the results themselves.

**tablename**
    The name of the table

**index**
    When the WHERE expression uses an indexed attribute, this allows you to
    manually specify which index name to use for the query. You will only need
    this if the constraints provided match more than one index.

**limit**
    The maximum number of items to evaluate (not necessarily the number of
    matching items).

**ORDER BY**
    Sort the results by a field.

.. warning::

    Using ORDER BY with LIMIT may produce unexpected results. If you use ORDER
    BY on the range key of the index you are querying on, it will work as
    expected. Otherwise, DQL will fetch the number of results specified by the
    LIMIT and then sort them.

**ASC | DESC**
    Sort the results in ASCending (the default) or DESCending order.

**SAVE**
    Save the results to a file. By default the items will be JSON-encoded, but
    if the filename ends with '.csv' they will be saved in CSV format instead.

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
