Getting Started
===============
Install DQL with pip::

    pip install dql

Since DQL uses :mod:`boto` under the hood, the authentication mechanism is the
same. You may either set the ``AWS_ACCESS_KEY_ID`` and
``AWS_SECRET_ACCESS_KEY`` environment variables, or pass them in on the command
line:

.. code-block:: bash

    $ dql -a <access key> -s <secret key>

DQL uses ``us-west-1`` as the default region. You can change this by setting
the ``AWS_REGION`` variable or passing it in on the command line:

.. code-block:: bash

    $ dql -r us-east-1

You can begin using DQL immediately. Try creating a table and inserting some
data

.. code-block:: sql

    us-west-1> CREATE TABLE posts (username STRING HASH KEY,
             >                     postid NUMBER RANGE KEY,
             >                     ts NUMBER INDEX('ts-index'))
             >                    THROUGHPUT (5, 5);
    us-west-1> INSERT INTO posts (username, postid, ts, text)
             > VALUES ('steve', 1, 1386413481, 'Hey guys!'),
             >        ('steve', 2, 1386413516, 'Guys?'),
             >        ('drdice', 3, 1386413575, 'Fun fact: dolphins are dicks');
    us-west-1> ls
    Name   Status  Read  Write
    posts  ACTIVE  5     5


You can query this data in a couple of ways. The first should look familiar

.. code-block:: sql

    us-west-1> SELECT * FROM posts WHERE username = 'steve';

This performs a table query using the hash key, so it should run relatively
quickly. The second way of accessing data is by using a scan

.. code-block:: sql

    us-west-1> SCAN posts FILTER username = 'steve';

This returns the same data as the SELECT, but it retrieves it with a table scan
instead, which is much slower.

You can also perform updates to the data in a familiar way

.. code-block:: sql

    us-west-1> UPDATE posts SET text = 'Hay gusys!!11' WHERE
             > username = 'steve' AND postid = 1;

The :ref:`queries` section has more details. Check it out for more information on
the performance implications and query options.
