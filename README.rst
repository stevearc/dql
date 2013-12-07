DQL
===
.. image:: https://travis-ci.org/mathcamp/dql.png?branch=master
  :target: https://travis-ci.org/mathcamp/dql
.. image:: https://coveralls.io/repos/mathcamp/dql/badge.png?branch=master
  :target: https://coveralls.io/r/mathcamp/dql?branch=master

A simple, SQL-ish language for DynamoDB

Getting Started
===============
Here are some basic examples to get you started::

    $ dql
    us-west-1> CREATE TABLE forum_threads (name STRING HASH KEY, subject STRING RANGE KEY) THROUGHPUT (4, 2)
    us-west-1> INSERT INTO forum_threads (name, subject, views, replies) VALUES ('Self Defense', 'Defense from Banana', 67, 4), ('Self Defense', 'Defense from Strawberry', 10, 0), ('Cheese Shop', 'Anyone seen the camembert?', 16, 1)
    us-west-1> SCAN forum_threads
    us-west-1> COUNT forum_threads WHERE name = 'Self Defense'
    us-west-1> UPDATE forum_threads SET views += 1 WHERE name = 'Self Defense' AND subject = 'Defense from Banana'
    us-west-1> SELECT * FROM forum_threads WHERE name = 'Self Defense'
    us-west-1> DELETE FROM forum_threads WHERE name = 'Cheese Shop'
    us-west-1> ALTER TABLE forum_threads SET THROUGHPUT (8, 4)
    us-west-1> DROP TABLE forum_threads

Development
===========
To get started developing dql, run the following command::

    wget https://raw.github.com/mathcamp/devbox/master/devbox/unbox.py && \
    python unbox.py git@github.com:mathcamp/dql

This will clone the repository and install the package into a virtualenv

Running Tests
-------------
The command to run tests is ``python setup.py nosetests``. Some of these tests
require `DynamoDB Local
<http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Tools.html>`_.
There is a nose plugin that will download and run the DynamoDB Local service
during the tests. It requires the java 7 runtime, so make sure you have that
installed.

TODO
====
* Select allows an ordering (reverse)
* Select allows consistent reads
* Engine should use table metadata to figure out the index name
* Full documentation
* More complex "where" clauses (queries limited to QUERY_OPERATORS, scans to FILTER_OPERATORS)
* Insert supports inserting items with different attrs
* Support for the 'set' data types
* CLI allows multi-line queries
* Create indexes with different projection mappings
* Fetch cloudwatch metrics during table describe
* Engine should have local scope for variable name resolution
* More unit test coverage
* CLI should autocomplete table names
