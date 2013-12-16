ALTER
=====

Synopsis
--------
.. code-block:: sql

    ALTER TABLE tablename
        SET
        [INDEX index]
        THROUGHPUT throughput

Examples
--------
.. code-block:: sql

    ALTER TABLE foobars SET THROUGHPUT (4, 8);
    ALTER TABLE foobars SET THROUGHPUT (7, 0);
    ALTER TABLE foobars SET INDEX ts-index THROUGHPUT (5, 0);

Description
-----------
Alter changes the read/write throughput on a table. You may not increase either
value to more than double the current value. You may also only decrease the
throughput twice per day. If you wish to change one of the throughput values
and not the other, pass in ``0`` for the value you wish to remain unchanged.

Parameters
----------
**tablename**
    The name of the table

**throughput**
    The read/write throughput in the form (*read_throughput*, *write_throughput*)

**index**
    The name of the global index
