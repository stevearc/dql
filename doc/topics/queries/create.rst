CREATE
======

Synopsis
--------
.. code-block:: sql

    CREATE TABLE
        [IF NOT EXISTS]
        tablename
        attributes
        [GLOBAL [ALL|KEYS|INCLUDE] INDEX global_index]

Examples
--------
.. code-block:: sql

    CREATE TABLE foobars (id STRING HASH KEY);
    CREATE TABLE IF NOT EXISTS foobars (id STRING HASH KEY);
    CREATE TABLE foobars (id STRING HASH KEY, foo BINARY RANGE KEY,
                          THROUGHPUT (1, 1));
    CREATE TABLE foobars (id STRING HASH KEY,
                          foo BINARY RANGE KEY,
                          ts NUMBER INDEX('ts-index'),
                          views NUMBER INDEX('views-index'));
    CREATE TABLE foobars (id STRING HASH KEY, bar STRING) GLOBAL INDEX
                         ('bar-index', bar, THROUGHPUT (1, 1));
    CREATE TABLE foobars (id STRING HASH KEY, baz NUMBER,
                          THROUGHPUT (2, 2))
                          GLOBAL INDEX ('bar-index', bar STRING, baz)
                          GLOBAL INCLUDE INDEX ('baz-index', baz, ['bar'], THROUGHPUT (4, 2));

Description
-----------
Create a new table. You must have exactly one hash key, zero or one range keys,
and up to five local indexes and five global indexes. You must have a range key
in order to have any local indexes.

Parameters
----------
**IF NOT EXISTS**
    If present, do not through an exception if the table already exists.

**tablename**
    The name of the table that you want to alter

**attributes**
    A list of attribute declarations of the format (*name* *data type* [*key
    type*]) The available data types are ``STRING``, ``NUMBER``, and ``BINARY``.
    You will not need to specify any other type, because these fields are only
    used for index creation and it is (presently) impossible to index anything
    other than these three.  The available key types are ``HASH KEY``, ``RANGE
    KEY``, and ``[ALL|KEYS|INCLUDE] INDEX(name)``. At the end of the attribute
    list you may specify the ``THROUGHPUT``, which is in the form of
    ``(read_throughput, write_throughput)``. If throughput is not specified it
    will default to ``(5, 5)``.

**global_index**
    A global index for the table. You may provide up to 5. The format is
    (*name*, *hash key*, [*range key*], [*non-key attributes*], [*throughput*]).
    If the hash/range key is in the **attributes** declaration, you don't need
    to supply a data type.. *non-key attributes* should only be provided if it
    is an ``INCLUDE`` index. If throughput is not specified it will default to
    ``(5, 5)``.

Schema Design at a Glance
-------------------------
When DynamoDB scales, it partitions based on the hash key. For this reason, all
queries (not scans) *must* include the hash key in the WHERE clause (and
optionally the range key or a local/global index). So keep that in mind as you
design your schema.

The keypair formed by the hash key and range key is referred to as the 'primary
key'. If there is no range key, the primary key is just the hash key. The
primary key is unique among items in the table. No two items may have the same
primary key.

From a query standpoint, local indexes behave nearly the same as a range key.
The main difference is that the hash key + range key pair doesn't have to be
unique.

Global indexes can be thought of as adding additional hash and range keys to
the table. They allow you to query a table on a different hash key than the one
defined on the table. Global indexes have throughput that is managed
independently of the table they are on. Global index keys do not have a
uniqueness constraint (there may be multiple items in the table that have the
same hash and range key).

Read Amazon's documentation for `Create Table
<http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_CreateTable.html>`_
for more information.
