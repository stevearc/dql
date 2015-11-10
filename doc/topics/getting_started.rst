Getting Started
===============
Install DQL with pip::

    pip install dql

Since DQL uses :mod:`botocore` under the hood, the authentication mechanism is
the same. You can use the ``$HOME/.aws/credentials`` file or set the environment
variables ``AWS_ACCESS_KEY_ID`` and ``AWS_SECRET_ACCESS_KEY``.

DQL uses ``us-west-1`` as the default region. You can change this by setting
the ``AWS_REGION`` variable or passing it in on the command line:

.. code-block:: bash

    $ dql -r us-east-1

You can begin using DQL immediately. Try creating a table and inserting some
data

.. code-block:: sql

    us-west-1> CREATE TABLE posts (username STRING HASH KEY,
             >                     postid NUMBER RANGE KEY,
             >                     ts NUMBER INDEX('ts-index'),
             >                     THROUGHPUT (5, 5));
    us-west-1> INSERT INTO posts (username, postid, ts, text)
             > VALUES ('steve', 1, 1386413481, 'Hey guys!'),
             >        ('steve', 2, 1386413516, 'Guys?'),
             >        ('drdice', 1, 1386413575, 'No one here');
    us-west-1> ls
    Name   Status  Read  Write
    posts  ACTIVE  5     5


You can query this data in a couple of ways. The first should look familiar

.. code-block:: sql

    us-west-1> SELECT * FROM posts WHERE username = 'steve';

By default, SELECT statements are only allowed to perform index queries, not
scan the table. You can enable scans by setting the 'allow_select_scan' option
(see :ref:`options`) or replacing SELECT with SCAN:

.. code-block:: sql

    us-west-1> SCAN * FROM posts WHERE postid = 2;

You can also perform updates to the data in a familiar way:

.. code-block:: sql

    us-west-1> UPDATE posts SET text = 'Hay gusys!!11' WHERE
             > username = 'steve' AND postid = 1;

The :ref:`queries` section has detailed information about each type of query.
