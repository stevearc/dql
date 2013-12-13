Advanced Usage
==============
This section is devoted to some of the advanced uses of DQL

API
---
DQL execution is available as an API. For example::

    import dql

    engine = dql.Engine()
    engine.connect_to_region('us-west-1')
    results = engine.execute("SCAN mytable LIMIT 10")
    for item in results:
        print dict(item)

The return value will vary based on the type of query.

Scope
-----
DQL supports the use of variables anywhere that you would otherwise have to
specify a data type. Create your scope as a dict and pass it in the the engine
with the commands::

    scope = {'foo1': 1, 'foo2': 2}
    engine.execute("INSERT INTO foobars (foo) VALUES (foo1), (foo2)"),
                   scope=scope)

The interactive client has a special way to modify the scope. You can switch
into 'code' mode to execute python, and then use that scope as the engine
scope::

    us-west-1> code
    >>> foo1 = 1
    >>> foo2 = 2
    >>> endcode
    us-west-1> INSERT INTO foobars (foo) VALUES (foo1), (foo2)
    success
