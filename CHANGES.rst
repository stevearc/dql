Changelog
=========

0.5.1
-----
* Pretty-format non-item query return values (such as count)
* Disable passing AWS credentials on the command line

0.5.0
-----
* **Breakage**: New syntax for SELECT, SCAN, UPDATE, DELETE
* **Breakage**: Removed COUNT query (now ``SELECT count(*)``)
* **Breakage**: Removed the ability to embed python in queries
* New alternative syntax for INSERT
* ALTER can create and drop global indexes
* Queries and updates now use the most recent DynamoDB expressions API
* Unified options in CLI under the ``opt`` command

0.4.1
-----
* Update to maintain compatibility with new versions of botocore and dynamo3
* Improving CloudWatch support (which is used to get consumed table capacity)

0.4.0
-----
* **Breakage**: Dropping support for python 3.2 due to lack of botocore support
* Feature: Support for JSON data types

0.3.2
-----
* Bug fix: Allow '.' in table names of DUMP SCHEMA command
* Bug fix: Passing a port argument to local connection doesn't crash
* Bug fix: Prompt says 'localhost' when connected to DynamoDB local

0.3.1
-----
* Bug fix: Allow '.' in table names

0.3.0
-----
* Feature: SELECT and COUNT can have FILTER clause
* Feature: FILTER clause may OR constraints together

0.2.1
-----
* Bug fix: Crash when printing 'COUNT' queries

0.2.0
-----
* Feature: Python 3 support

0.1.0
-----
* First public release
