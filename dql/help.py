""" Help text for the CLI """

ALTER = """
    Alter a table's throughput or create/drop global indexes

    ALTER TABLE tablename
        SET [INDEX index] THROUGHPUT throughput
    ALTER TABLE tablename
        DROP INDEX index [IF EXISTS]
    ALTER TABLE tablename
        CREATE GLOBAL [ALL|KEYS|INCLUDE] INDEX global_index [IF NOT EXISTS]

    Examples
    --------
    ALTER TABLE foobars SET THROUGHPUT (4, 8);
    ALTER TABLE foobars SET THROUGHPUT (7, *);
    ALTER TABLE foobars SET INDEX ts-index THROUGHPUT (5, *);
    ALTER TABLE foobars SET INDEX ts-index THROUGHPUT (5, *);
    ALTER TABLE foobars DROP INDEX ts-index;
    ALTER TABLE foobars DROP INDEX ts-index IF EXISTS;
    ALTER TABLE foobars CREATE GLOBAL INDEX ('ts-index', ts NUMBER, THROUGHPUT (5, 5));
    ALTER TABLE foobars CREATE GLOBAL INDEX ('ts-index', ts NUMBER) IF NOT EXISTS;
"""

ANALYZE = """
    Run a query and print out the consumed capacity

    ANALYZE query

    Examples
    --------
    ANALYZE SELECT * FROM foobars WHERE id = 'a';
    ANALYZE INSERT INTO foobars (id, name) VALUES (1, 'dsa');
    ANALYZE DELETE FROM foobars KEYS IN ('foo', 'bar'), ('baz', 'qux');
"""

CREATE = """
    Create a new table

    CREATE TABLE
        [IF NOT EXISTS]
        tablename
        attributes
        [GLOBAL [ALL|KEYS|INCLUDE] INDEX global_index]

    Examples
    --------
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
"""

DELETE = """
    Delete items from a table

    DELETE FROM
        tablename
        [ KEYS IN primary_keys ]
        [ WHERE expression ]
        [ USING index ]

    Examples
    --------
    DELETE FROM foobars; -- This will delete all items in the table!
    DELETE FROM foobars WHERE foo != 'bar' AND baz >= 3;
    DELETE FROM foobars KEYS IN 'hkey1', 'hkey2' WHERE attribute_exists(foo);
    DELETE FROM foobars KEYS IN ('hkey1', 'rkey1'), ('hkey2', 'rkey2');
    DELETE FROM foobars WHERE (foo = 'bar' AND baz >= 3) USING baz-index;

    Links
    -----
    http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.SpecifyingConditions.html#ConditionExpressionReference
"""

DROP = """
    Delete a table

    DROP TABLE
        [ IF EXISTS ]
        tablename

    Examples
    --------
    DROP TABLE foobars;
    DROP TABLE IF EXISTS foobars;
"""

DUMP = """
    Print the schema creation statements for your tables

    DUMP SCHEMA [ tablename [, ...] ]

    Examples
    --------
    DUMP SCHEMA;
    DUMP SCHEMA foobars, widgets;
"""

EXPLAIN = """
    Print out the DynamoDB queries that will be executed for a command

    EXPLAIN query

    Examples
    --------
    EXPLAIN SELECT * FROM foobars WHERE id = 'a';
    EXPLAIN INSERT INTO foobars (id, name) VALUES (1, 'dsa');
    EXPLAIN DELETE FROM foobars KEYS IN ('foo', 'bar'), ('baz', 'qux');
"""

INSERT = """
    Insert data into a table

    INSERT INTO tablename
        attributes VALUES values
    INSERT INTO tablename
        items

    Examples
    --------
    INSERT INTO foobars (id) VALUES (1);
    INSERT INTO foobars (id, bar) VALUES (1, 'hi'), (2, 'yo');
    INSERT INTO foobars (id='foo', bar=10);
    INSERT INTO foobars (id='foo'), (id='bar', baz=(1, 2, 3));
"""

LOAD = """
    Load data from a file (saved with SELECT ... SAVE) into a table

    LOAD filename INTO tablename

    Examples
    --------
    LOAD archive.p INTO mytable;
    LOAD dump.json.gz INTO mytable;
"""

SCAN = SELECT = """
    Select items from a table by querying an index

    SELECT
        [ CONSISTENT ]
        attributes
        FROM tablename
        [ KEYS IN primary_keys | WHERE expression ]
        [ USING index ]
        [ LIMIT limit ]
        [ SCAN LIMIT scan_limit ]
        [ ORDER BY field ]
        [ ASC | DESC ]
        [ SAVE file.json ]

    Examples
    --------
    SELECT * FROM foobars SAVE out.p;
    SELECT * FROM foobars WHERE foo = 'bar';
    SELECT count(*) FROM foobars WHERE foo = 'bar';
    SELECT id, TIMESTAMP(updated) FROM foobars KEYS IN 'id1', 'id2';
    SELECT * FROM foobars KEYS IN ('hkey', 'rkey1'), ('hkey', 'rkey2');
    SELECT CONSISTENT * foobars WHERE foo = 'bar' AND baz >= 3;
    SELECT * foobars WHERE foo = 'bar' AND attribute_exists(baz);
    SELECT * foobars WHERE foo = 1 AND NOT (attribute_exists(bar) OR contains(baz, 'qux'));
    SELECT 10 * (foo - bar) FROM foobars WHERE id = 'a' AND ts < 100 USING ts-index;
    SELECT * FROM foobars WHERE foo = 'bar' LIMIT 50 DESC;
    SELECT * FROM foobars THROTTLE (50%, *);

    Links
    -----
    http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.SpecifyingConditions.html#ConditionExpressionReference
"""

UPDATE = """
    Update items in a table

    UPDATE tablename
        update_expression
        [ KEYS IN primary_keys ]
        [ WHERE expression ]
        [ USING index ]
        [ RETURNS (NONE | ( ALL | UPDATED) (NEW | OLD)) ]

    Examples
    --------
    UPDATE foobars SET foo = 'a';
    UPDATE foobars SET foo = 'a', bar = bar + 4 WHERE id = 1 AND foo = 'b';
    UPDATE foobars SET foo = if_not_exists(foo, 'a') RETURNS ALL NEW;
    UPDATE foobars SET foo = list_append(foo, 'a') WHERE size(foo) < 3;
    UPDATE foobars ADD foo 1, bar 4;
    UPDATE foobars ADD fooset (1, 2);
    UPDATE foobars REMOVE old_attribute;
    UPDATE foobars DELETE fooset (1, 2);

    Links
    -----
    http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.Modifying.html#Expressions.Modifying.UpdateExpressions
    http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.SpecifyingConditions.html#ConditionExpressionReference
"""

OPTIONS = """
    Get or set options

    use 'opt <option>' to get the value of option, and 'opt <option> <value>'
    to set the option.

                width : int, The number of characters wide to format the display
             pagesize : int, The number of results to get per page for queries
              display : (less|stdout), The reader used to view query results
               format : (smart|column|expanded), Display format for query results
    allow_select_scan : bool, If True, SELECT statements can perform table scans
"""
