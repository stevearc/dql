""" Execution engine """
from boto.dynamodb2.fields import HashKey, RangeKey, AllIndex
from boto.dynamodb2.table import Table
from boto.dynamodb2.items import Item
from boto.dynamodb2.types import (NUMBER, STRING, BINARY, NUMBER_SET,
                                  STRING_SET, BINARY_SET, Dynamizer)
from boto.exception import JSONResponseError

from .models import TableMeta
from .grammar import parser


OPS = {
    '=': 'eq',
    '!=': 'ne',
    '>': 'gt',
    '>=': 'gte',
    '<': 'lt',
    '<=': 'lte',
}

TYPES = {
    'NUMBER': NUMBER,
    'STRING': STRING,
    'BINARY': BINARY,
    'NUMBER_SET': NUMBER_SET,
    'STRING_SET': STRING_SET,
    'BINARY_SET': BINARY_SET,
}


class LossyFloatDynamizer(Dynamizer):

    """ Use float/int instead of Decimal for numeric types """

    def _encode_n(self, attr):
        if isinstance(attr, bool):
            return str(int(attr))
        return str(attr)

    def _encode_ns(self, attr):
        return [str(i) for i in attr]

    def _decode_n(self, attr):
        try:
            return int(attr)
        except ValueError:
            return float(attr)

    def _decode_ns(self, attr):
        return set(map(self._decode_n, attr))


class Engine(object):

    """
    DQL execution engine

    Parameters
    ----------
    connection : :class:`boto.dynamodb2.layer1.DynamoDBConnection`

    """

    def __init__(self,  connection):
        self._connection = connection
        self._metadata = {}
        self.dynamizer = LossyFloatDynamizer()
        self.lossy_dynamizer = LossyFloatDynamizer()

    @property
    def connection(self):
        """ Get the dynamo connection """
        return self._connection

    @connection.setter
    def connection(self, connection):
        """ Change the dynamo connection """
        self._connection = connection
        self._metadata = {}

    def describe_all(self):
        """ Describe all tables in the connected region """
        tables = self.connection.list_tables()['TableNames']
        descs = []
        for tablename in tables:
            descs.append(self.describe(tablename, True))
        return descs

    def describe(self, tablename, refresh=False):
        """ Get the :class:`.TableMeta` for a table """
        if refresh or tablename not in self._metadata:
            desc = self.connection.describe_table(tablename)
            self._metadata[tablename] = TableMeta.from_description(desc)
        return self._metadata[tablename]

    def execute(self, commands):
        """ Run """
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

    def resolve(self, val):
        """ Resolve a value into a string or number """
        if val.getName() == 'identifier':
            # TODO: have a local scope to look up variables
            raise NotImplementedError
        elif val.getName() == 'number':
            try:
                return int(val.number)
            except ValueError:
                return float(val.number)
        elif val.getName() == 'str':
            return val.str[1:-1]
        elif val.getName() == 'null':
            return None
        raise SyntaxError("Unable to resolve value '%s'" % val)

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
            for key, op, val in tree.where:
                kwargs[key + '__' + OPS[op]] = self.resolve(val)
                if key in desc.indexes:
                    kwargs['index'] = desc.indexes[key].index_name
            if tree.limit:
                kwargs['limit'] = self.resolve(tree.limit[1])
            if tree.using:
                kwargs['index'] = self.resolve(tree.using[1])
            if tree.attrs.asList() != ['*']:
                kwargs['attributes'] = tree.attrs.asList()
            if tree.order == 'DESC':
                kwargs['reverse'] = True

            return table.query(**kwargs)

    def _scan(self, tree):
        """ Run a SCAN statement """
        tablename = tree.table
        kwargs = {}
        for key, op, val in tree.filter:
            kwargs[key + '__' + OPS[op]] = self.resolve(val)
        if tree.limit:
            kwargs['limit'] = self.resolve(tree.limit[1])

        table = Table(tablename, connection=self.connection)
        return table.scan(**kwargs)

    def _count(self, tree):
        """ Run a COUNT statement """
        tablename = tree.table
        desc = self.describe(tablename)
        kwargs = {}
        for key, op, val in tree.where:
            kwargs[key + '__' + OPS[op]] = self.resolve(val)
            if key in desc.indexes:
                kwargs['index'] = desc.indexes[key].index_name
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
            keys = list(self._iter_where_in(tree))
            results = table.batch_get(keys=keys)
        else:
            for key, op, val in tree.where:
                kwargs[key + '__' + OPS[op]] = self.resolve(val)
                if key in desc.indexes:
                    kwargs['index'] = desc.indexes[key].index_name
            if tree.using:
                kwargs['index'] = self.resolve(tree.using[1])
            results = table.query(**kwargs)

        count = 0
        with table.batch_write() as batch:
            for item in results:
                # Pull out just the hash and range key from the item
                kwargs = {desc.hash_key.name: item[desc.hash_key.name]}
                if desc.range_key is not None:
                    kwargs[desc.range_key.name] = item[desc.range_key.name]
                count += 1
                batch.delete_item(**kwargs)
        return 'deleted %d items' % count

    def _update(self, tree):
        """ Run an UPDATE statement """
        tablename = tree.table
        table = Table(tablename, connection=self.connection)
        desc = self.describe(tablename)
        updates = {}
        result = []

        if tree.returns:
            returns = '_'.join(tree.returns)
        else:
            returns = 'NONE'

        for field, op, val in tree.updates:
            value = self.resolve(val)
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
            else:
                raise SyntaxError("Unknown operation '%s'" % op)
            updates[field] = {'Action': action}
            if action != 'DELETE':
                updates[field]['Value'] = self.lossy_dynamizer.encode(value)

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
                ret = self.connection.update_item(tablename, key, updates,
                                                  return_values=returns)
                if returns != 'NONE':
                    result.append(decode_result(ret))
        else:
            # We're updating THE WHOLE TABLE
            for item in table.scan():
                key = encode_pkey(desc.primary_key(item))
                ret = self.connection.update_item(tablename, key, updates,
                                                  return_values=returns)
                if returns != 'NONE':
                    result.append(decode_result(ret))
        if returns == 'NONE':
            return None
        return result

    def _create(self, tree):
        """ Run a SELECT statement """
        tablename = tree.table
        schema = []
        indexes = []
        hash_key = None
        for name, type_, index in tree.attrs:
            if index[0] == 'HASH':
                hash_key = HashKey(name, data_type=TYPES[type_])
                schema.append(hash_key)
            elif index[0] == 'RANGE':
                schema.append(RangeKey(name, data_type=TYPES[type_]))
            else:
                index_name = self.resolve(index[1])
                indexes.append(AllIndex(index_name, parts=[
                    hash_key,
                    RangeKey(name, data_type=TYPES[type_])
                ]))

        if tree.throughput:
            throughput = {
                'read': self.resolve(tree.throughput[0]),
                'write': self.resolve(tree.throughput[1]),
            }
        else:
            throughput = None

        try:
            Table.create(tablename, schema=schema, indexes=indexes,
                         throughput=throughput, connection=self.connection)
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

    def _alter(self, tree):
        """ Run an ALTER statement """
        tablename = tree.table
        table = Table(tablename, connection=self.connection)
        throughput = {
            'read': self.resolve(tree.throughput[0]),
            'write': self.resolve(tree.throughput[1]),
        }
        table.update(throughput=throughput)
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
