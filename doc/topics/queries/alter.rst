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
    ALTER TABLE foobars SET THROUGHPUT (7, *);
    ALTER TABLE foobars SET INDEX ts-index THROUGHPUT (5, *);

Description
-----------
Alter changes the read/write throughput on a table. Dynamo stipulates that you
may only increase the throughput by a factor of 2 at a time. If you attempt to
increase the throughput by more than that, DQL will have to make multiple
calls, waiting for the updates to go through in the interim. You may only
decrease the throughput twice per day. If you wish to change one of the
throughput values and not the other, pass in ``0`` or ``*`` for the value you
wish to remain unchanged.

Parameters
----------
**tablename**
    The name of the table

**throughput**
    The read/write throughput in the form (*read_throughput*, *write_throughput*)

**index**
    The name of the global index
