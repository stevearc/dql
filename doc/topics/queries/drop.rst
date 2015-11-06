DROP
====

Synopsis
--------
.. code-block:: sql

    DROP TABLE
        [ IF EXISTS ]
        tablename

Examples
--------
.. code-block:: sql

    DROP TABLE foobars;
    DROP TABLE IF EXISTS foobars;

Description
-----------
Deletes a table and all its items.

.. warning::

    This action cannot be undone! Treat the same way you treat ``rm -rf``

Parameters
----------
**IF EXISTS**
    If present, do not raise an exception if the table does not exist.

**tablename**
    The name of the table
