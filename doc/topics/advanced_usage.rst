Advanced Usage
==============
This section is devoted to some of the advanced uses of DQL

API
---
DQL execution is available as a python library. For example:

.. code-block:: python

    import dql

    engine = dql.Engine()
    engine.connect_to_region('us-west-1')
    results = engine.execute("SCAN mytable LIMIT 10")
    for item in results:
        print dict(item)

The return value will vary based on the type of query.

Embedded Python
---------------
DQL supports the use of python expressions anywhere that you would otherwise
have to specify a data type. Just surround the python with backticks. Create
your variable scope as a dict and pass it to the engine with the commands:

.. code-block:: python

    scope = {'foo1': 1, 'foo2': 2}
    engine.execute("INSERT INTO foobars (foo) VALUES (`foo1`), (`foo2`)"),
                   scope=scope)

The interactive client has a special way to modify the scope. You can switch
into 'code' mode to execute python, and then use that scope as the engine
scope:

.. code-block:: sql

    us-west-1> code
    >>> foo1 = 1
    >>> import time
    >>> endcode
    us-west-1> INSERT INTO foobars (foo) VALUES (`foo1`), (`time.time()`)
    success

You can also spread the expressions across multiple lines by using the format
``m`<expr>```. If you do this, you will need to ``return`` a value. The
difference here is between using ``eval`` and ``exec`` in python.

.. code-block:: sql

    us-west-1> UPDATE foobars SET foo = m`
             > if bar % 2 == 0:
             >     return bar**2
             > else:
             >     return bar - 1
             > `;
