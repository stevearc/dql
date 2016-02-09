LOAD
====

Synopsis
--------
.. code-block:: sql

    LOAD filename INTO tablename
        [ THROTTLE throughput ]

Examples
--------
.. code-block:: sql

    LOAD archive.p INTO mytable;
    LOAD dump.json.gz INTO mytable;

Description
-----------
Take the results of a ``SELECT ... SAVE outfile`` and insert all of the records
into a table.

Parameters
----------
**filename**
    The file containing the records to upload

**tablename**
    The name of the table(s) to upload the records into

**THROTTLE**
    Limit the amount of throughput this query can consume. This is a pair of
    values for ``(read_throughput, write_throughput)``. You can use a flat
    number or a percentage (e.g. ``20`` or ``50%``). Using ``*`` means no limit
    (typically useless unless you have set a default throttle in the
    :ref:`options`).
