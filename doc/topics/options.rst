.. _options:

Options
=======
The following are options you can set for DQL. Options are set with ``opt
<option> <value>``, and you can see the current option value with ``opt
<option>``

+-------------------+---------------------------+-----------------------------------------------------+
|             width | int / auto                | Number of characters wide to format the display     |
+-------------------+---------------------------+-----------------------------------------------------+
|          pagesize | int / auto                | Number of results to get per page for queries       |
+-------------------+---------------------------+-----------------------------------------------------+
|           display | less / stdout             | The reader used to view query results               |
+-------------------+---------------------------+-----------------------------------------------------+
|            format | smart / column / expanded | Display format for query results                    |
+-------------------+---------------------------+-----------------------------------------------------+
| allow_select_scan | bool                      | If True, SELECT statement can perform table scans   |
+-------------------+---------------------------+-----------------------------------------------------+

Throttling
----------
DQL also allows you to be careful how much throughput you consume with your
queries. Use the ``throttle`` command to set persistent limits on all or some of
your tables/indexes. Some examples::

    # Set the total allowed throughput across all tables
    > throttle 1000 100
    # Set the default allowed throughput per-table/index
    > throttle default 40% 20%
    # Set the allowed throughput on a table
    > throttle mytable 10 10
    # Set the allowed throughput on a global index
    > throttle mytable myindex 40 6

See ``help throttle`` for more details, and use ``unthrottle`` to remove a
throttle. You can also set throttles on a per-query basis with the ``THROTTLE``
keyword.
