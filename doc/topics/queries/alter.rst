ALTER
=====

Synopsis
--------
.. code-block:: sql

    ALTER TABLE tablename
        SET [INDEX index] THROUGHPUT throughput
    ALTER TABLE tablename
        DROP INDEX index
    ALTER TABLE tablename
        CREATE GLOBAL [ALL|KEYS|INCLUDE] INDEX global_index

Examples
--------
.. code-block:: sql

    ALTER TABLE foobars SET THROUGHPUT (4, 8);
    ALTER TABLE foobars SET THROUGHPUT (7, *);
    ALTER TABLE foobars SET INDEX ts-index THROUGHPUT (5, *);
    ALTER TABLE foobars SET INDEX ts-index THROUGHPUT (5, *);
    ALTER TABLE foobars DROP INDEX ts-index;
    ALTER TABLE foobars CREATE GLOBAL INDEX ('ts-index', ts NUMBER, THROUGHPUT (5, 5));

Description
-----------
Alter changes the read/write throughput on a table. You may only
decrease the throughput on a table four times per day (see :ref:`the AWS docs on
limits
<http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Limits.html>`.
If you wish to change one of the throughput values and not the other, pass in
``0`` or ``*`` for the value you wish to remain unchanged.

Parameters
----------
**tablename**
    The name of the table

**throughput**
    The read/write throughput in the form (*read_throughput*, *write_throughput*)

**index**
    The name of the global index
