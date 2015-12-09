.. _data_types:

Data Types
==========
Below is a list of all DynamoDB data types and examples of how to represent
those types in queries.

+------------+---------------------------+
| NUMBER     | ``123``                   |
+------------+---------------------------+
| STRING     | ``'asdf'`` or ``"asdf"``  |
+------------+---------------------------+
| BINARY     | ``b'datadatadata'``       |
+------------+---------------------------+
| NUMBER SET | ``(1, 2, 3)``             |
+------------+---------------------------+
| STRING SET | ``('a', 'b', 'c')``       |
+------------+---------------------------+
| BINARY SET | ``(b'a', b'c')``          |
+------------+---------------------------+
| BOOL       | ``TRUE`` or ``FALSE``     |
+------------+---------------------------+
| LIST       | ``[1, 2, 3]``             |
+------------+---------------------------+
| MAP        | ``{'a': 1}``              |
+------------+---------------------------+

Timestamps
----------
DQL has some limited support for timestamp types. These will all be converted to
Unix timestamps under the hood.

* ``TIMESTAMP('2015-12-3 13:32:00')`` or ``TS()`` - parses the timestamp in your local timezone
* ``UTCTIMESTAMP('2015-12-3 13:32:00')`` or ``UTCTS()`` - parses the timestamp as UTC
* ``NOW()`` - Returns the current timestamp

You can also add/subtract intervals from a timestamp

* ``NOW() - INTERVAL("1 day")``
* ``NOW() + INTERVAL "1y 2w -5 minutes"``

You can wrap any of these with ``MS()`` to convert the result into milliseconds
instead of seconds.

``MS(NOW() + INTERVAL '2 days')``

Below is a list of all keywords that you can use for intervals

* ``year``, ``years``, ``y``
* ``month``, ``months``
* ``week``, ``weeks``, ``w``
* ``day``, ``days``, ``d``
* ``hour``, ``hours``, ``h``
* ``minute``, ``minutes``, ``m``
* ``second``, ``seconds``, ``s``
* ``millisecond``, ``milliseconds``, ``ms``
* ``microsecond``, ``microseconds``, ``us``
