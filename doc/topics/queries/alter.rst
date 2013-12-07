ALTER
=====

Synopsis
--------
.. code-block:: sql

    ALTER TABLE tablename
        SET THROUGHPUT throughput

Examples
--------
.. code-block:: sql

    ALTER TABLE foobars SET THROUGHPUT (4, 8);

Description
-----------
Alter changes the read/write throughput on a table. You may not increase either
value to more than double the current value. You may also only decrease the
throughput twice per day.

Parameters
----------
**tablename**
    The name of the table

**throughput**
    The read/write throughput in the form (*read_throughput*, *write_throughput*)
