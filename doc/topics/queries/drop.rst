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

    DROP TABLE foobars
    DROP TABLE IF EXISTS foobars

Description
-----------
Deletes a table and all its items.

.. warning::

    This is not reversible! Use with extreme caution!

Parameters
----------
**IF EXISTS**
    If present, do not through an exception if the table does not exist.

**tablename**
    The name of the table
