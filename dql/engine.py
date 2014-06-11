""" Execution engine """
import time

import logging
import six
from dateutil.parser import parse
from decimal import Decimal
from pyparsing import ParseException

from .grammar import parser, line_parser
from .models import TableMeta
from dynamo3 import (TYPES, DynamoDBConnection, DynamoKey, LocalIndex,
                     GlobalIndex, DynamoDBError, ItemUpdate, Binary,
                     Throughput)


LOG = logging.getLogger(__name__)


OPS = {
    '=': 'eq',
    '!=': 'ne',
    '>': 'gt',
    '>=': 'gte',
    '<': 'lt',
    '<=': 'lte',
    'BEGINS WITH': 'beginswith',
    'IN': 'in',
    'CONTAINS': 'contains',
    'NOT CONTAINS': 'ncontains',
}


class Engine(object):

    """
    DQL execution engine

    Parameters
    ----------
    connection : :class:`~dynamo3.DynamoDBConnection`, optional
        If not present, you will need to call :meth:`.Engine.connect_to_region`

    Attributes
    ----------
    scope : dict
        Lookup scope for variables

    Notes
    -----
    One of the advanced features of the engine is the ability to set variables
    which can later be referenced in queries. Whenever a STRING or NUMBER is
    expected in a query, you may alternatively supply a variable name. That
    name will be looked up in the engine's ``scope``. This allows you to do
    things like::

        engine.scope['myfoo'] = 'abc'
        engine.execute("SELECT * FROM foobars WHERE foo = myfoo")

    """

    def __init__(self, connection=None):
        self._connection = connection
        self.cached_descriptions = {}
        self._cloudwatch_connection = None
        self.scope = {}

    def connect_to_region(self, region, *args, **kwargs):
        """ Connect the engine to an AWS region """
        self.connection = DynamoDBConnection.connect_to_region(region, *args,
                                                               **kwargs)

    def connect_to_host(self, *args, **kwargs):
        """ Connect the engine to a specific host """
        self.connection = DynamoDBConnection.connect_to_host(*args, **kwargs)

    @property
    def connection(self):
        """ Get the dynamo connection """
        return self._connection

    @connection.setter
    def connection(self, connection):
        """ Change the dynamo connection """
        self._connection = connection
        self._cloudwatch_connection = None
        self.cached_descriptions = {}

    @property
    def cloudwatch_connection(self):
        """ Lazy create a connection to cloudwatch """
        if self._cloudwatch_connection is None:
            session = self.connection.service.session
            self._cloudwatch_connection = \
                session.get_service('cloudwatch')
        return self._cloudwatch_connection

    def describe_all(self):
        """ Describe all tables in the connected region """
        tables = self.connection.list_tables()
        descs = []
        for tablename in tables:
            descs.append(self.describe(tablename, True))
        return descs

    def _get_metric(self, tablename, metric):
        """ Fetch a read/write capacity metric """
        end = time.time()
        begin = end - 20 * 60  # 20 minute window
        op = self.cloudwatch_connection.get_operation('get_metric_statistics')
        kwargs = {
            'period': 60,
            'start_time': begin,
            'end_time': end,
            'metric_name': metric,
            'namespace': 'AWS/DynamoDB',
            'statistics': ['Sum'],
            'dimensions': [{'Name': 'TableName', 'Value': tablename}],
        }
        endpoint = self.cloudwatch_connection.get_endpoint(
            self.connection.region)
        data = op.call(endpoint, **kwargs)[1]
        points = data['Datapoints']
        if len(points) < 2:
            return 0
        else:
            points.sort(key=lambda r: parse(r['Timestamp']))
            start, end = points[-2:]
            time_range = parse(end['Timestamp']) - parse(start['Timestamp'])
            total = end['Sum']
            return total / float(time_range.total_seconds())

    def get_capacity(self, tablename):
        """ Get the consumed read/write capacity """
        # If we're connected to a DynamoDB Local instance, don't connect to the
        # actual cloudwatch endpoint
        if self.connection.region == 'local':
            return 0, 0
        return (self._get_metric(tablename, 'ConsumedReadCapacityUnits'),
                self._get_metric(tablename, 'ConsumedWriteCapacityUnits'))

    def describe(self, tablename, refresh=False, metrics=False):
        """ Get the :class:`.TableMeta` for a table """
        if refresh or tablename not in self.cached_descriptions:
            desc = self.connection.call('DescribeTable', table_name=tablename)
            table = TableMeta.from_description(desc)
            self.cached_descriptions[tablename] = table
            if metrics:
                read, write = self.get_capacity(tablename)
                table.consumed_read_capacity = read
                table.consumed_write_capacity = write

        return self.cached_descriptions[tablename]

    def execute(self, commands):
        """
        Parse and run a DQL string

        Parameters
        ----------
        commands : str
            The DQL command string

        """
        tree = parser.parseString(commands)
        for statement in tree:
            result = self._run(statement)
        return result

    def _run(self, tree):
        """ Run a query from a parse tree """
        if tree.action == 'SELECT':
            return self._select(tree)
        elif tree.action == 'SCAN':
            return self._scan(tree)
        elif tree.action == 'COUNT':
            return self._count(tree)
        elif tree.action == 'DELETE':
            return self._delete(tree)
        elif tree.action == 'UPDATE':
            return self._update(tree)
        elif tree.action == 'CREATE':
            return self._create(tree)
        elif tree.action == 'INSERT':
            return self._insert(tree)
        elif tree.action == 'DROP':
            return self._drop(tree)
        elif tree.action == 'ALTER':
            return self._alter(tree)
        elif tree.action == 'DUMP':
            return self._dump(tree)
        else:
            raise SyntaxError("Unrecognized action '%s'" % tree.action)

    def resolve(self, val, scope=None):
        """ Resolve a value into a string or number """
        if scope is None:
            scope = self.scope
        if val.getName() == 'python':
            if val.python[0].lower() == 'm':
                code = val.python[2:-1]
                func_def = 'def __dql_func():'
                lines = [4 * ' ' + line for line in code.splitlines()]
                lines.insert(0, func_def)
                expr_func = '\n'.join(lines)
                LOG.debug("Exec and run:\n%s", expr_func)
                six.exec_(expr_func, scope)
                return scope['__dql_func']()
            else:
                code = val.python[1:-1]
                return eval(code, scope)
        elif val.getName() == 'number':
            try:
                return int(val.number)
            except ValueError:
                return Decimal(val.number)
        elif val.getName() == 'str':
            return val.str[1:-1]
        elif val.getName() == 'null':
            return None
        elif val.getName() == 'binary':
            return Binary(val.binary[2:-1])
        elif val.getName() == 'set':
            if val.set == '()':
                return set()
            return set([self.resolve(v) for v in val.set])
        raise SyntaxError("Unable to resolve value '%s'" % val)

    def _where_kwargs(self, desc, clause, index=True):
        """ Generate dynamo3 kwargs from a where clause """
        kwargs = {}
        all_keys = set()
        if not index:
            for conjunction in clause[3::2]:
                if conjunction != clause[1]:
                    raise SyntaxError("Cannot mix AND and OR inside FILTER clause")
            clause = clause[0::2]
        for key, op, val in clause:
            if key in all_keys:
                raise SyntaxError("Cannot use a field more than once in a FILTER clause")
            all_keys.add(key)
            if op == 'BETWEEN':
                kwargs[key + '__between'] = (self.resolve(val[0]),
                                             self.resolve(val[1]))
            elif op == 'IS':
                if val == 'NULL':
                    kwargs[key + '__null'] = True
                elif val == 'NOT NULL':
                    kwargs[key + '__null'] = False
                else:
                    raise SyntaxError("Well this is odd: %s" % val)
            else:
                kwargs[key + '__' + OPS[op]] = self.resolve(val)
        if index:
            if desc.hash_key.name in all_keys:
                for key in all_keys:
                    field = desc.attrs[key]
                    if field.key_type == 'INDEX':
                        kwargs['index'] = field.index_name
                        break
            else:
                # Must be using a global index
                for index in six.itervalues(desc.global_indexes):
                    if index.hash_key.name in all_keys:
                        kwargs['index'] = index.name
                        break
        return kwargs

    def _iter_where_in(self, tree):
        """ Iterate over the WHERE KEYS IN and generate primary keys """
        desc = self.describe(tree.table)
        for keypair in tree.where:
            yield desc.primary_key(*map(self.resolve, keypair))

    def _select(self, tree):
        """ Run a SELECT statement """
        tablename = tree.table
        desc = self.describe(tablename)
        kwargs = {}
        if tree.consistent:
            kwargs['consistent'] = True
        if tree.attrs.asList() != ['*']:
            kwargs['attributes'] = tree.attrs.asList()

        if tree.keys_in:
            if tree.limit:
                raise SyntaxError("Cannot use LIMIT with WHERE KEYS IN")
            elif tree.using:
                raise SyntaxError("Cannot use USING with WHERE KEYS IN")
            elif tree.filter:
                raise SyntaxError("Cannot use FILTER with WHERE KEYS IN")
            keys = list(self._iter_where_in(tree))
            return self.connection.batch_get(tablename, keys=keys, **kwargs)
        else:
            kwargs.update(self._where_kwargs(desc, tree.where))
            if tree.limit:
                kwargs['limit'] = self.resolve(tree.limit[1])
            if tree.using:
                kwargs['index'] = self.resolve(tree.using[1])
            if tree.filter:
                kwargs['filter'] = self._where_kwargs(self.describe(tablename),
                                                      tree.filter, index=False)
                if len(tree.filter) > 1:
                    kwargs['filter_or'] = tree.filter[1] == 'OR'

            kwargs['desc'] = tree.order == 'DESC'

            return self.connection.query(tablename, **kwargs)

    def _scan(self, tree):
        """ Run a SCAN statement """
        tablename = tree.table
        kwargs = self._where_kwargs(self.describe(tablename), tree.filter,
                                    index=False)
        if len(tree.filter) > 1:
            kwargs['filter_or'] = tree.filter[1] == 'OR'
        if tree.limit:
            kwargs['limit'] = self.resolve(tree.limit[1])

        return self.connection.scan(tablename, **kwargs)

    def _count(self, tree):
        """ Run a COUNT statement """
        tablename = tree.table
        desc = self.describe(tablename)
        kwargs = self._where_kwargs(desc, tree.where)
        if tree.using:
            kwargs['index'] = self.resolve(tree.using[1])
        if tree.consistent:
            kwargs['consistent'] = True
        if tree.filter:
            kwargs['filter'] = self._where_kwargs(self.describe(tablename),
                                                  tree.filter, index=False)
            if len(tree.filter) > 1:
                kwargs['filter_or'] = tree.filter[1] == 'OR'

        return self.connection.query(tablename, count=True, **kwargs)

    def _delete(self, tree):
        """ Run a DELETE statement """
        tablename = tree.table
        kwargs = {}
        desc = self.describe(tablename)

        # We can't do a delete by group, so we have to select first
        if tree.keys_in:
            if tree.using:
                raise SyntaxError("Cannot use USING with WHERE KEYS IN")
            results = list(self._iter_where_in(tree))
        else:
            kwargs.update(self._where_kwargs(desc, tree.where))
            if tree.using:
                attributes = [desc.hash_key.name]
                if desc.range_key is not None:
                    attributes.append(desc.range_key.name)
                kwargs['attributes'] = attributes
                kwargs['index'] = self.resolve(tree.using[1])
            results = self.connection.query(tablename, **kwargs)

        count = 0
        with self.connection.batch_write(tablename) as batch:
            for item in results:
                # Pull out just the hash and range key from the item
                pkey = {desc.hash_key.name: item[desc.hash_key.name]}
                if desc.range_key is not None:
                    pkey[desc.range_key.name] = item[desc.range_key.name]
                count += 1
                batch.delete(pkey)
        return 'deleted %d items' % count

    def _update(self, tree):
        """ Run an UPDATE statement """
        tablename = tree.table
        desc = self.describe(tablename)
        result = []

        if tree.returns:
            returns = '_'.join(tree.returns)
        else:
            returns = 'NONE'

        def get_updates(item=None):
            """ Construct a list of values to update """
            updates = []
            scope = None
            if item is not None:
                scope = self.scope.copy()
                scope['row'] = dict(item)
                scope.update(item.items())
            for field, op, val in tree.updates:
                value = self.resolve(val, scope)
                if value is None:
                    if op != '=':
                        raise SyntaxError(
                            "Cannot increment/decrement by NULL!")
                    action = ItemUpdate.DELETE
                elif op == '=':
                    action = ItemUpdate.PUT
                elif op == '+=':
                    action = ItemUpdate.ADD
                elif op == '-=':
                    action = ItemUpdate.ADD
                    value = -value
                elif op == '<<':
                    action = ItemUpdate.ADD
                    if not isinstance(value, set):
                        value = set([value])
                elif op == '>>':
                    action = ItemUpdate.DELETE
                    if not isinstance(value, set):
                        value = set([value])
                else:
                    raise SyntaxError("Unknown operation '%s'" % op)
                updates.append(ItemUpdate(action, field, value))
            return updates

        if tree.keys_in:
            updates = get_updates()
            for key in self._iter_where_in(tree):
                ret = self.connection.update_item(tablename, key, updates,
                                                  returns=returns)
                if ret:
                    result.append(ret)
        elif tree.where:
            kwargs = {}
            for key, op, val in tree.where:
                kwargs[key + '__' + OPS[op]] = self.resolve(val)
            for item in self.connection.query(tablename, **kwargs):
                key = desc.primary_key(item)
                updates = get_updates(item)
                ret = self.connection.update_item(tablename, key, updates,
                                                  returns=returns)
                if ret:
                    result.append(ret)
        else:
            # We're updating THE WHOLE TABLE
            for item in self.connection.scan(tablename):
                key = desc.primary_key(item)
                updates = get_updates(item)
                ret = self.connection.update_item(tablename, key, updates,
                                                  returns=returns)
                if ret:
                    result.append(ret)
        if result:
            return result

    def _create(self, tree):
        """ Run a SELECT statement """
        tablename = tree.table
        attrs = []
        indexes = []
        global_indexes = []
        hash_key = None
        range_key = None
        attrs = {}
        for declaration in tree.attrs:
            name, type_ = declaration[:2]
            if len(declaration) > 2:
                index = declaration[2]
            else:
                index = None
            if index is not None:
                if index[0] == 'HASH':
                    field = hash_key = DynamoKey(name, data_type=TYPES[type_])
                elif index[0] == 'RANGE':
                    field = range_key = DynamoKey(name, data_type=TYPES[type_])
                else:
                    index_type = index[0]
                    kwargs = {}
                    if index_type[0] in ('ALL', 'INDEX'):
                        factory = LocalIndex.all
                    elif index_type[0] == 'KEYS':
                        factory = LocalIndex.keys
                    elif index_type[0] == 'INCLUDE':
                        factory = LocalIndex.include
                        kwargs['includes'] = [self.resolve(i) for i in
                                              index.include]
                    index_name = self.resolve(index[1])
                    field = DynamoKey(name, data_type=TYPES[type_])
                    idx = factory(index_name, field, **kwargs)
                    indexes.append(idx)
            else:
                field = DynamoKey(name, data_type=TYPES[type_])
            attrs[field.name] = field

        for gindex in tree.global_indexes:
            index_type, name, var1 = gindex[:3]
            g_hash_key = attrs[var1]
            g_range_key = None
            kwargs = {}
            for piece in gindex[3:]:
                if isinstance(piece, six.string_types):
                    g_range_key = attrs[piece]
                else:
                    kwargs['throughput'] = Throughput(*map(self.resolve,
                                                           piece.throughput))

            if index_type[0] in ('ALL', 'INDEX'):
                factory = GlobalIndex.all
            elif index_type[0] == 'KEYS':
                factory = GlobalIndex.keys
            elif index_type[0] == 'INCLUDE':
                factory = GlobalIndex.include
                kwargs['includes'] = [self.resolve(i) for i in gindex.include]
            index = factory(self.resolve(name), g_hash_key, g_range_key,
                            **kwargs)
            global_indexes.append(index)

        throughput = None
        if tree.throughput:
            throughput = Throughput(*map(self.resolve, tree.throughput))

        try:
            self.connection.create_table(
                tablename, hash_key, range_key, indexes=indexes,
                global_indexes=global_indexes, throughput=throughput)
        except DynamoDBError as e:
            if e.kwargs['Code'] == 'ResourceInUseException' or tree.not_exists:
                return "Table '%s' already exists" % tablename
            raise
        return "Created table '%s'" % tablename

    def _insert(self, tree):
        """ Run an INSERT statement """
        tablename = tree.table
        keys = tree.attrs
        count = 0
        with self.connection.batch_write(tablename) as batch:
            for values in tree.data:
                if len(keys) != len(values):
                    raise SyntaxError("Values '%s' do not match attributes "
                                      "'%s'" % (values, keys))
                data = dict(zip(keys, map(self.resolve, values)))
                batch.put(data)
                count += 1
        return "Inserted %d items" % count

    def _drop(self, tree):
        """ Run a DROP statement """
        tablename = tree.table
        try:
            self.connection.delete_table(tablename)
        except DynamoDBError as e:
            if e.kwargs['Code'] == 'ResourceNotFoundException' and tree.exists:
                return "Table '%s' does not exist" % tablename
            raise
        return "Dropped table '%s'" % tablename

    def _set_throughput(self, tablename, read, write, index_name=None):
        """ Set the read/write throughput on a table or global index """

        throughput = Throughput(read, write)
        if index_name:
            self.connection.update_table(tablename,
                                         global_indexes={
                                             index_name: throughput,
                                         })
        else:
            self.connection.update_table(tablename, throughput)

    def _alter(self, tree):
        """ Run an ALTER statement """
        tablename = tree.table

        def get_desc():
            """ Get the table or global index description """
            desc = self.describe(tablename, refresh=True)
            if tree.index:
                return desc.global_indexes[tree.index]
            return desc
        desc = get_desc()

        def num_or_star(value):
            """ * maps to 0, all other values resolved """
            return 0 if value == '*' else self.resolve(value)
        read = num_or_star(tree.throughput[0])
        write = num_or_star(tree.throughput[1])
        if read <= 0:
            read = desc.read_throughput
        if write <= 0:
            write = desc.write_throughput
        while desc.read_throughput != read or desc.write_throughput != write:
            next_read = min(read, 2 * desc.read_throughput)
            next_write = min(write, 2 * desc.write_throughput)
            self._set_throughput(tablename, next_read, next_write, tree.index)
            desc = get_desc()
            while desc.status == 'UPDATING':  # pragma: no cover
                time.sleep(5)
                desc = get_desc()
        return 'success'

    def _dump(self, tree):
        """ Run a DUMP statement """
        schema = []
        if tree.tables:
            for table in tree.tables:
                schema.append(self.describe(table, True).schema)
        else:
            for table in self.describe_all():
                schema.append(table.schema)

        return '\n\n'.join(schema)


class FragmentEngine(Engine):

    """
    A DQL execution engine that can handle query fragments

    """

    def __init__(self, connection):
        super(FragmentEngine, self).__init__(connection)
        self.fragments = ''
        self.last_query = ''

    @property
    def partial(self):
        """ True if there is a partial query stored """
        return len(self.fragments) > 0

    def reset(self):
        """ Clear any query fragments from the engine """
        self.fragments = ''

    def execute(self, fragment):
        """
        Run or aggregate a query fragment

        Concat the fragment to any stored fragments. If they form a complete
        query, run it and return the result. If not, store them and return
        None.

        """
        self.fragments = (self.fragments + '\n' + fragment).lstrip()
        try:
            line_parser.parseString(self.fragments)
        except ParseException:
            pass
        else:
            self.last_query = self.fragments.strip()
            self.fragments = ''
            return super(FragmentEngine, self).execute(self.last_query)
        return None

    def pformat_exc(self, exc):
        """ Format an exception message for the last query's parse error """
        lines = []
        try:
            pre_nl = self.last_query.rindex('\n', 0, exc.loc) + 1
        except ValueError:
            pre_nl = 0
        try:
            post_nl = self.last_query.index('\n', exc.loc)
        except ValueError:
            post_nl = len(self.last_query)
        lines.append(self.last_query[:post_nl])
        lines.append(' ' * (exc.loc - pre_nl) + '^')
        lines.append(str(exc))
        return '\n'.join(lines)
