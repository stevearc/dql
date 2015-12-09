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
| allow_select_scan | bool                      | If True, SELECT statements can perform table scans  |
+-------------------+---------------------------+-----------------------------------------------------+
