EXPLAIN
=======

Synopsis
--------
.. code-block:: sql

    EXPLAIN query


Examples
--------
.. code-block:: sql

    EXPLAIN SELECT * FROM foobars WHERE id = 'a';
    EXPLAIN INSERT INTO foobars (id, name) VALUES (1, 'dsa');
    EXPLAIN DELETE FROM foobars KEYS IN ('foo', 'bar'), ('baz', 'qux');

Description
-----------
This is a meta-query that will print out debug information. It will not make any
actual requests except for possibly a ``DescribeTable`` if the primary key or
indexes are needed to build the query. The output of the EXPLAIN will be the
name of the DynamoDB Action(s) that will be called, and the parameters passed up
in the request. You can use this to preview exactly what DQL will do before it
happens.
