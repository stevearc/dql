ANALYZE
=======

Synopsis
--------
.. code-block:: sql

    ANALYZE query


Examples
--------
.. code-block:: sql

    ANALYZE SELECT * FROM foobars WHERE id = 'a';
    ANALYZE INSERT INTO foobars (id, name) VALUES (1, 'dsa');
    ANALYZE DELETE FROM foobars KEYS IN ('foo', 'bar'), ('baz', 'qux');

Description
-----------
You can prefix any query that will read or write data with ANALYZE and after
running the query it will print out how much capacity was consumed at every part
of the query.
