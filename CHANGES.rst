Changelog
=========

0.5.18
------
* Bug fix: Correct name substitution/selection logic
* Swapped out ``bin/run_dql.py`` for ``bin/install.py``. Similar concept, better execution.

0.5.17
------
* Bug fix: Can't display Binary data

0.5.16
------
* Bug fix: Can't use boolean values in update statements

0.5.15
------
* Gracefully handle missing imports on Windows

0.5.14
------
* Missing curses library won't cause ImportError

0.5.13
------
* Fix bug where query would sometimes display 'No Results' even when results were found.

0.5.12
------
* Differentiate LIMIT and SCAN LIMIT
* Options and query syntax for ``throttling`` the consumed throughput
* Crash fixes and other small robustness improvements

0.5.11
------
* SELECT <attributes> can now use full expressions

0.5.10
------
* LOAD command to insert records from a file created with ``SELECT ... SAVE``
* Default SAVE format is pickle
* SAVE command can gzip the file

0.5.9
-----
* Don't print results to console when saving to a file
* 'auto' pagesize to adapt to terminal height
* When selecting specific attributes with KEYS IN only those attributes are fetched
* ORDER BY queries spanning multiple pages no longer stuck on first page
* Column formatter fits column widths more intelligently
* Smart formatter is smarter about switching to Expanded mode

0.5.8
-----
* Tab completion for Mac OS X

0.5.7
-----
* ``run_dql.py`` locks in a version
* Display output auto-detects terminal width

0.5.6
-----
* Format option saves properly
* WHERE expressions can compare fields to fields (e.g. ``WHERE foo > bar``)
* Always perform batch_get after querying/scanning an index that doesn't project all attributes

0.5.5
-----
* General bug fixes
* Self contained ``run_dql.py`` script

0.5.4
-----
* Fixes for ``watch`` display
* SELECT can save the results to a file

0.5.3
-----
* ALTER commands can specify IF (NOT) EXISTS
* New ``watch`` command to monitor table consumed capacities
* SELECT can fetch attributes that aren't projected onto the queried index
* SELECT can ORDER BY non-range-key attributes

0.5.2
-----
* EXPLAIN <query> will print out the DynamoDB calls that will be made when you run the query
* ANALYZE <query> will run the query and print out consumed capacity information

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
