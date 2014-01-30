""" Execution engine """
import time
from datetime import datetime, timedelta

import boto.dynamodb.types
import boto.dynamodb2
import logging
from boto.dynamodb.types import Binary
from boto.dynamodb2.fields import (BaseSchemaField, HashKey, RangeKey,
                                   AllIndex, KeysOnlyIndex, IncludeIndex,
                                   GlobalAllIndex, GlobalKeysOnlyIndex,
                                   GlobalIncludeIndex)
from boto.dynamodb2.items import Item
from boto.dynamodb2.table import Table
from boto.dynamodb2.types import (NUMBER, STRING, BINARY, NUMBER_SET,
                                  STRING_SET, BINARY_SET, Dynamizer)
from boto.ec2.cloudwatch import connect_to_region
from boto.exception import JSONResponseError
from decimal import Decimal, Inexact, Rounded
from pyparsing import ParseException

from .grammar import parser, line_parser
from .models import TableMeta


LOG = logging.getLogger(__name__)

# HACK to force conversion of floats to Decimals, even if inexact
boto.dynamodb.types.DYNAMODB_CONTEXT.traps[Inexact] = False
boto.dynamodb.types.DYNAMODB_CONTEXT.traps[Rounded] = False


def float_to_decimal(f):  # pragma: no cover
    """ Monkey-patched replacement for boto's broken version """
    n, d = f.as_integer_ratio()
    numerator, denominator = Decimal(n), Decimal(d)
    ctx = boto.dynamodb.types.DYNAMODB_CONTEXT
    return ctx.divide(numerator, denominator)

boto.dynamodb.types.float_to_decimal = float_to_decimal


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

TYPES = {
    'NUMBER': NUMBER,
    'STRING': STRING,
    'BINARY': BINARY,
    'NUMBER_SET': NUMBER_SET,
    'STRING_SET': STRING_SET,
    'BINARY_SET': BINARY_SET,
}


class Engine(object):

    """
    DQL execution engine

    Parameters
    ----------
    connection : :class:`~boto.dynamodb2.layer1.DynamoDBConnection`, optional
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
        self.dynamizer = Dynamizer()
        self._cloudwatch_connection = None
        self.scope = {}

    def connect_to_region(self, region, **kwargs):
        """ Connect the engine to an AWS region """
        self.connection = boto.dynamodb2.connect_to_region(region, **kwargs)

    @property
    def connection(self):
        """ Get the dynamo connection """
        return self._connection

    @property
    def cloudwatch_connection(self):
        """ Lazy create a connection to cloudwatch """
        if self._cloudwatch_connection is None:
            self._cloudwatch_connection = \
                connect_to_region(
                    self.connection.region.name,
                    aws_access_key_id=self.connection.aws_access_key_id,
                    aws_secret_access_key=self.connection.aws_secret_access_key)
        return self._cloudwatch_connection

    @connection.setter
    def connection(self, connection):
        """ Change the dynamo connection """
        self._connection = connection
        self._cloudwatch_connection = None
        self.cached_descriptions = {}

    def describe_all(self):
        """ Describe all tables in the connected region """
        tables = self.connection.list_tables()['TableNames']
        descs = []
        for tablename in tables:
            descs.append(self.describe(tablename, True))
        return descs

    def _get_metric(self, tablename, metric):
        """ Fetch a read/write capacity metric """
        end = datetime.now()
        begin = end - timedelta(minutes=5)
        m = self.cloudwatch_connection.get_metric_statistics(
            60, begin, end, metric, 'AWS/DynamoDB', ['Sum'],
            {'TableName': [tablename]})
        if len(m) == 0:
            return 0
        else:
            return m[0]['Sum'] / float((end - begin).total_seconds())

    def get_capacity(self, tablename):
        """ Get the consumed read/write capacity """
        if self.connection.region.name == 'local':
            return 0, 0
        return (self._get_metric(tablename, 'ConsumedReadCapacityUnits'),
                self._get_metric(tablename, 'ConsumedWriteCapacityUnits'))

    def describe(self, tablename, refresh=False, metrics=False):
        """ Get the :class:`.TableMeta` for a table """
        if refresh or tablename not in self.cached_descriptions:
            desc = self.connection.describe_table(tablename)
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
                exec expr_func in scope
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
        """ Generate boto kwargs from a where clause """
        kwargs = {}
        all_keys = []
        for key, op, val in clause:
            all_keys.append(key)
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
                for index in desc.global_indexes.itervalues():
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
        table = Table(tablename, connection=self.connection)
        desc = self.describe(tablename)
        kwargs = {}
        if tree.consistent:
            kwargs['consistent'] = True

        if tree.keys_in:
            if tree.limit:
                raise SyntaxError("Cannot use LIMIT with WHERE KEYS IN")
            elif tree.using:
                raise SyntaxError("Cannot use USING with WHERE KEYS IN")
            elif tree.attrs.asList() != ['*']:
                raise SyntaxError("Must SELECT * when using WHERE KEYS IN")
            keys = list(self._iter_where_in(tree))
            return table.batch_get(keys=keys, **kwargs)
        else:
            kwargs.update(self._where_kwargs(desc, tree.where))
            if tree.limit:
                kwargs['limit'] = self.resolve(tree.limit[1])
            if tree.using:
                kwargs['index'] = self.resolve(tree.using[1])
            if tree.attrs.asList() != ['*']:
                kwargs['attributes'] = tree.attrs.asList()
            kwargs['reverse'] = tree.order != 'DESC'

            return table.query(**kwargs)

    def _scan(self, tree):
        """ Run a SCAN statement """
        tablename = tree.table
        kwargs = self._where_kwargs(self.describe(tablename), tree.filter,
                                    index=False)
        if tree.limit:
            kwargs['limit'] = self.resolve(tree.limit[1])

        table = Table(tablename, connection=self.connection)
        return table.scan(**kwargs)

    def _count(self, tree):
        """ Run a COUNT statement """
        tablename = tree.table
        desc = self.describe(tablename)
        kwargs = self._where_kwargs(desc, tree.where)
        if tree.using:
            kwargs['index'] = self.resolve(tree.using[1])
        if tree.consistent:
            kwargs['consistent'] = True

        table = Table(tablename, connection=self.connection)
        return table.query_count(**kwargs)

    def _delete(self, tree):
        """ Run a DELETE statement """
        tablename = tree.table
        table = Table(tablename, connection=self.connection)
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
            results = table.query(**kwargs)

        count = 0
        with table.batch_write() as batch:
            for item in results:
                # Pull out just the hash and range key from the item
                pkey = {desc.hash_key.name: item[desc.hash_key.name]}
                if desc.range_key is not None:
                    pkey[desc.range_key.name] = item[desc.range_key.name]
                count += 1
                batch.delete_item(**pkey)
        return 'deleted %d items' % count

    def _update(self, tree):
        """ Run an UPDATE statement """
        tablename = tree.table
        table = Table(tablename, connection=self.connection)
        desc = self.describe(tablename)
        result = []

        if tree.returns:
            returns = '_'.join(tree.returns)
        else:
            returns = 'NONE'

        def get_update_dict(item=None):
            """ Construct a dict of values to update """
            updates = {}
            scope = None
            if item is not None:
                scope = self.scope.copy()
                scope['row'] = dict(item)
                scope.update(item.items())
            for field, op, val in tree.updates:
                value = self.resolve(val, scope)
                if value is None:
                    if op != '=':
                        raise SyntaxError("Cannot increment/decrement by NULL!")
                    action = 'DELETE'
                elif op == '=':
                    action = 'PUT'
                elif op == '+=':
                    action = 'ADD'
                elif op == '-=':
                    action = 'ADD'
                    value = -value
                elif op == '<<':
                    action = 'ADD'
                    if not isinstance(value, set):
                        value = set([value])
                elif op == '>>':
                    action = 'DELETE'
                    if not isinstance(value, set):
                        value = set([value])
                else:
                    raise SyntaxError("Unknown operation '%s'" % op)
                updates[field] = {'Action': action}
                if action != 'DELETE' or op in ('<<', '>>'):
                    updates[field]['Value'] = self.dynamizer.encode(value)
            return updates

        def encode_pkey(pkey):
            """ HACK: boto doesn't encode primary keys in update_item """
            return dict([(k, self.dynamizer.encode(v)) for k, v in
                         pkey.iteritems()])

        def decode_result(result):
            """ Create an Item from a raw return result """
            data = dict([(k, self.dynamizer.decode(v)) for k, v in
                         result.get('Attributes', {}).iteritems()])
            item = Item(table, data=data)
            return item

        if tree.keys_in:
            updates = get_update_dict()
            for key in self._iter_where_in(tree):
                key = encode_pkey(key)
                ret = self.connection.update_item(tablename, key, updates,
                                                  return_values=returns)
                if returns != 'NONE':
                    result.append(decode_result(ret))
        elif tree.where:
            kwargs = {}
            for key, op, val in tree.where:
                kwargs[key + '__' + OPS[op]] = self.resolve(val)
            for item in table.query(**kwargs):
                key = encode_pkey(desc.primary_key(item))
                updates = get_update_dict(item)
                ret = self.connection.update_item(tablename, key, updates,
                                                  return_values=returns)
                if returns != 'NONE':
                    result.append(decode_result(ret))
        else:
            # We're updating THE WHOLE TABLE
            for item in table.scan():
                key = encode_pkey(desc.primary_key(item))
                updates = get_update_dict(item)
                ret = self.connection.update_item(tablename, key, updates,
                                                  return_values=returns)
                if returns != 'NONE':
                    result.append(decode_result(ret))
        if returns == 'NONE':
            return None
        return (item for item in result)

    def _create(self, tree):
        """ Run a SELECT statement """
        tablename = tree.table
        attrs = []
        schema = []
        indexes = []
        global_indexes = []
        hash_key = None
        raw_attrs = {}
        for declaration in tree.attrs:
            name, type_ = declaration[:2]
            if len(declaration) > 2:
                index = declaration[2]
            else:
                index = None
            if index is not None:
                if index[0] == 'HASH':
                    field = hash_key = HashKey(name, data_type=TYPES[type_])
                    schema.insert(0, field.schema())
                elif index[0] == 'RANGE':
                    field = RangeKey(name, data_type=TYPES[type_])
                    schema.append(field.schema())
                else:
                    index_type = index[0]
                    kwargs = {}
                    if index_type[0] in ('ALL', 'INDEX'):
                        idx_class = AllIndex
                    elif index_type[0] == 'KEYS':
                        idx_class = KeysOnlyIndex
                    elif index_type[0] == 'INCLUDE':
                        idx_class = IncludeIndex
                        kwargs['includes'] = [self.resolve(i) for i in
                                              index.include]
                    index_name = self.resolve(index[1])
                    field = RangeKey(name, data_type=TYPES[type_])
                    idx = idx_class(index_name, parts=[hash_key, field],
                                    **kwargs)
                    indexes.append(idx.schema())
            else:
                field = BaseSchemaField(name, data_type=TYPES[type_])
            attrs.append(field.definition())
            raw_attrs[name] = field

        for gindex in tree.global_indexes:
            index_type, name, var1 = gindex[:3]
            hash_key = HashKey(var1, data_type=raw_attrs[var1].data_type)
            parts = [hash_key]
            throughput = None
            for piece in gindex[3:]:
                if isinstance(piece, basestring):
                    range_key = RangeKey(piece,
                                         data_type=raw_attrs[piece].data_type)
                    parts.append(range_key)
                else:
                    throughput = piece.throughput
            read, write = 5, 5
            if throughput:
                read, write = map(self.resolve, throughput)

            if index_type[0] in ('ALL', 'INDEX'):
                idx_class = GlobalAllIndex
            elif index_type[0] == 'KEYS':
                idx_class = GlobalKeysOnlyIndex
            elif index_type[0] == 'INCLUDE':
                idx_class = GlobalIncludeIndex
            index = idx_class(self.resolve(name), parts=parts)
            index.throughput = {
                'read': read,
                'write': write,
            }
            if gindex.include:
                index.includes_fields = [self.resolve(i) for i in
                                         gindex.include]
            s = index.schema()
            global_indexes.append(s)

        read, write = 5, 5
        if tree.throughput:
            read, write = map(self.resolve, tree.throughput)
        throughput = {
            'ReadCapacityUnits': read,
            'WriteCapacityUnits': write,
        }

        # Make sure indexes & global indexes either have data or are None
        indexes = indexes or None
        global_indexes = global_indexes or None
        try:
            self.connection.create_table(
                attrs, tablename, schema, throughput,
                local_secondary_indexes=indexes,
                global_secondary_indexes=global_indexes)
        except JSONResponseError as e:
            if e.status != 400 or not tree.not_exists:
                raise
        return "Created table '%s'" % tablename

    def _insert(self, tree):
        """ Run an INSERT statement """
        tablename = tree.table
        keys = tree.attrs
        table = Table(tablename, connection=self.connection)
        count = 0
        with table.batch_write() as batch:
            for values in tree.data:
                if len(keys) != len(values):
                    raise SyntaxError("Values '%s' do not match attributes "
                                      "'%s'" % (values, keys))
                data = dict(zip(keys, [self.resolve(v) for v in values]))
                batch.put_item(data=data, overwrite=True)
                count += 1
        return "Inserted %d items" % count

    def _drop(self, tree):
        """ Run a DROP statement """
        tablename = tree.table
        table = Table(tablename, connection=self.connection)
        try:
            table.delete()
        except JSONResponseError as e:
            if e.status != 400 or not tree.exists:
                raise
        return "Dropped table '%s'" % tablename

    def _set_throughput(self, tablename, read, write, index_name=None):
        """ Set the read/write throughput on a table or global index """

        throughput = {
            'ReadCapacityUnits': read,
            'WriteCapacityUnits': write,
        }
        if index_name:
            update = {
                'Update': {
                    'IndexName': index_name,
                    'ProvisionedThroughput': throughput,
                }
            }
            self.connection.update_table(tablename,
                                         global_secondary_index_updates=[
                                             update,
                                         ])
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
