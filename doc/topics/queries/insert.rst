INSERT
======

Synopsis
--------
.. code-block:: sql

    INSERT INTO tablename
        attributes
        VALUES values

Examples
--------
.. code-block:: sql

    INSERT INTO foobars (id) VALUES (1);
    INSERT INTO foobars (id, bar) VALUES (1, 'hi'), (2, 'yo');

Description
-----------
Insert data into a table

Parameters
----------
**tablename**
    The name of the table

**attributes**
    Comma-separated list of attribute names

**values**
    Comma-separated list of data to insert. The data is of the form *(var [,
    var]...)* and must contain the same number of items as the **attributes**
    parameter.

Data Types
----------
Below are examples of how to represent the different dynamo data types in DQL

**NUMBER**: ``123``
**STRING**: ``'abc'`` or ``"abc"``
**BINARY**: ``b'abc'`` or ``b"abc"``
**NUMBER SET**: ``(1, 2, 3)``
**STRING SET**: ``('a', 'b', 'c')``
**BINARY SET**: ``(b'a', b'b', b'c')``
