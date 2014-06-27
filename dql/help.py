""" Help text for the CLI """

ALTER = """
    Alter a table's throughput

    ALTER TABLE tablename
        SET
        [INDEX index]
        THROUGHPUT throughput

    Examples:
        ALTER TABLE foobars SET THROUGHPUT (7, *);
        ALTER TABLE foobars SET INDEX ts-index THROUGHPUT (5, 3);
"""

COUNT = """
    Count number of items in a table

    COUNT
        [ CONSISTENT ]
        tablename
        WHERE expression
        [ FILTER expression ]
        [ USING index ]

    Examples:
        COUNT foobars WHERE foo = 'bar';
        COUNT foobars WHERE foo = 'bar' FILTER baz IN (1, 3, 5);
        COUNT CONSISTENT foobars WHERE foo != 'bar' AND baz >= 3;
        COUNT foobars WHERE (foo = 'bar' AND baz >= 3) USING 'baz-index';
"""

CREATE = """
    Create a new table

    CREATE TABLE
        [IF NOT EXISTS]
        tablename
        attributes
        [GLOBAL [ALL|KEYS|INCLUDE] INDEX global_index]

    Examples:
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
        CREATE TABLE foobars (id STRING HASH KEY, bar STRING, baz NUMBER,
                            THROUGHPUT (2, 2))
                            GLOBAL INDEX ('bar-index', bar, baz)
                            GLOBAL INDEX ('baz-index', baz, THROUGHPUT (4, 2));
"""

DELETE = """
    Delete items from a table

    DELETE FROM
        tablename
        WHERE expression
        [ USING index ]

    Examples:
        DELETE FROM foobars WHERE foo = 'bar';
        DELETE FROM foobars WHERE foo != 'bar' AND baz >= 3;
        DELETE FROM foobars WHERE KEYS IN ('hkey1'), ('hkey2');
        DELETE FROM foobars WHERE KEYS IN ('hkey1', 'rkey1'), ('hkey2', 'rkey2');
        DELETE FROM foobars WHERE (foo = 'bar' AND baz >= 3) USING 'baz-index';
"""

DROP = """
    Delete a table

    DROP TABLE
        [ IF EXISTS ]
        tablename

    Examples:
        DROP TABLE foobars;
        DROP TABLE IF EXISTS foobars;
"""

DUMP = """
    Print the schema creation statements for your tables

    DUMP SCHEMA [ tablename [, ...] ]

    Examples:
        DUMP SCHEMA;
        DUMP SCHEMA foobars, widgets;
"""

INSERT = """
    Insert data into a table

    INSERT INTO tablename
        attributes
        VALUES values

    Examples:
        INSERT INTO foobars (id) VALUES (1);
        INSERT INTO foobars (id, bar) VALUES (1, 'hi'), (2, 'yo');
"""

SCAN = """
    Iterate over all items in a table

    SCAN tablename
        [ FILTER expression ]
        [ LIMIT limit ]

    Examples:
        SCAN foobars;
        SCAN foobars FILTER id = 'a' AND foo = 4;
        SCAN foobars FILTER id = 'a' AND foo CONTAINS 4 LIMIT 100;
        SCAN foobars FILTER id = 'a' OR foo IS NULL;
"""

SELECT = """
    Select items from a table by querying an index

    SELECT
        [ CONSISTENT ]
        attributes
        FROM tablename
        WHERE expression
        [ FILTER expression ]
        [ USING index ]
        [ LIMIT limit ]
        [ ASC | DESC ]

    Examples:
        SELECT * FROM foobars WHERE foo = 'bar';
        SELECT CONSISTENT * foobars WHERE foo = 'bar' AND baz >= 3;
        SELECT * foobars WHERE foo = 'bar' AND baz > 3 FILTER data IS NOT NULL;
        SELECT foo, bar FROM foobars WHERE id = 'a' AND ts < 100 USING 'ts-index';
        SELECT * FROM foobars WHERE foo = 'bar' AND baz >= 3 LIMIT 50 DESC;
"""

UPDATE = """
    Update items in a table

    UPDATE tablename
        SET values
        [ WHERE expression ]
        [ RETURNS (NONE | ( ALL | UPDATED) (NEW | OLD)) ]

    Examples:
        UPDATE foobars SET foo = 'a';
        UPDATE foobars SET foo = 'a', bar += 4 WHERE id = 1 AND foo = 'b';
        UPDATE foobars SET foo = 'a', bar += 4 RETURNS ALL NEW;
        UPDATE foobars SET myset << (5, 6, 7), mytags << 'new tag' WHERE KEYS IN ('a', 'b');
        UPDATE foobars SET foo = `bar + 1`;
"""
