LOAD
====

Synopsis
--------
.. code-block:: sql

    LOAD filename INTO tablename

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
