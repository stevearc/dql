CREATE
======

Synopsis
--------
.. code-block:: sql

    CREATE TABLE
        [ IF NOT EXISTS ]
        tablename
        attributes
        [ THROUGHPUT throughput ]

Examples
--------
.. code-block:: sql

    CREATE TABLE foobars (id STRING HASH KEY)
    CREATE TABLE IF NOT EXISTS foobars (id STRING HASH KEY)
    CREATE TABLE foobars (id STRING HASH KEY, foo BINARY RANGE KEY) THROUGHPUT (1, 1)
    CREATE TABLE foobars (id STRING HASH KEY, foo BINARY RANGE KEY, ts NUMBER INDEX('ts-index'), views NUMBER INDEX('views-index'))

Description
-----------
Create a new table. You must have exactly one hash key, zero or one range keys,
and up to five indexes. You must have a range key in order to have any indexes.

Parameters
----------
**IF NOT EXISTS**
    If present, do not through an exception if the table already exists.

**tablename**
    The name of the table that you want to alter

**attributes**
    A list of attribute declarations of the format (*name* *data type* *key type*)
    The available data types are ``STRING``, ``NUMBER``, and ``BINARY`` (DQL
    does not support the SET types yet). The available key types are ``HASH
    KEY``, ``RANGE KEY``, and ``INDEX(name)``.

**throughput**
    The read/write throughput in the form (*read_throughput*,
    *write_throughput*). If not present it will default to ``(5, 5)``

Schema Design at a Glance
-------------------------
When DynamoDB scales, it partitions based on the hash key. For this reason, all
of your table queries *must* include the hash key in the where clause (and
optionally the range key or an index). So keep that in mind as you design your
schema.

The keypair formed by the hash key and range key is referred to as the 'primary
key'. If there is no range key, it's just the hash key. The primary key is
unique among items in the table. No two items may have the same hash key and
range key.

From a query standpoint, indexes behave nearly the same as a range key. The
main difference is that they do not need to be unique.

Read Amazon's documentation for `Create Table
<http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_CreateTable.html>`_
for more invormation.
