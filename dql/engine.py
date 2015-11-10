""" Execution engine """
import time

import six
import botocore
import logging
from botocore.exceptions import ClientError
from dynamo3 import (TYPES, DynamoDBConnection, DynamoKey, LocalIndex,
                     GlobalIndex, DynamoDBError, Throughput, CheckFailed,
                     IndexUpdate)
from pyparsing import ParseException

from .constants import RESERVED_WORDS
from .expressions import ConstraintExpression, UpdateExpression, Visitor
from .grammar import parser, line_parser
from .models import TableMeta, Count
from .util import resolve


LOG = logging.getLogger(__name__)


def add_query_kwargs(kwargs, visitor, constraints, index):
    """ Construct KeyConditionExpression and FilterExpression """
    (query_const, filter_const) = constraints.remove_index(index)
    kwargs['key_condition_expr'] = query_const.build(visitor)
    if filter_const:
        kwargs['filter'] = filter_const.build(visitor)
    if index.name != 'TABLE':
        kwargs['index'] = index.name


def iter_insert_items(tree):
    """ Iterate over the items to insert from an INSERT statement """
    if tree.list_values:
        keys = tree.attrs
        for values in tree.list_values:
            if len(keys) != len(values):
                raise SyntaxError("Values '%s' do not match attributes "
                                  "'%s'" % (values, keys))
            yield dict(zip(keys, map(resolve, values)))
    elif tree.map_values:
        for item in tree.map_values:
            data = {}
            for (key, val) in item:
                data[key] = resolve(val)
            yield data
    else:
        raise SyntaxError("No insert data found")


def plural(value, append='s'):
    """ Helper function for pluralizing text """
    return '' if value == 1 else append


def pretty_format(statement, result):
    """ Format the return value of a query for humans """
    if result is None:
        return 'Success'
    if statement.action in ('SELECT', 'SCAN'):
        if isinstance(result, Count):
            if result == result.scanned_count:
                return "%d" % result
            else:
                return "%d (scanned count: %d)" % (result,
                                                   result.scanned_count)
    elif statement.action == 'UPDATE':
        if isinstance(result, six.integer_types):
            return "Updated %d item%s" % (result, plural(result))
    elif statement.action == 'DELETE':
        return "Deleted %d item%s" % (result, plural(result))
    elif statement.action == 'CREATE':
        if result:
            return "Created table %r" % statement.table
        else:
            return "Table %r already exists" % statement.table
    elif statement.action == 'INSERT':
        return "Inserted %d item%s" % (result, plural(result))
    elif statement.action == 'DROP':
        if result:
            return "Dropped table %r" % statement.table
        else:
            return "Table %r does not exist" % statement.table
    return result


class Engine(object):

    """
    DQL execution engine

    Parameters
    ----------
    connection : :class:`~dynamo3.DynamoDBConnection`, optional
        If not present, you will need to call :meth:`.Engine.connect`

    """

    def __init__(self, connection=None):
        self._connection = connection
        self.cached_descriptions = {}
        self._cloudwatch_connection = None
        self.allow_select_scan = False
        self.reserved_words = RESERVED_WORDS

    def connect(self, *args, **kwargs):
        """ Proxy to DynamoDBConnection.connect. """
        self.connection = DynamoDBConnection.connect(*args, **kwargs)
        self._session = kwargs.get('session')
        if self._session is None:
            self._session = botocore.session.get_session()

    @property
    def connection(self):
        """ Get the dynamo connection """
        return self._connection

    @property
    def region(self):
        """ Get the connected dynamo region or host """
        return self._connection.region

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
            conn = self._session.create_client('cloudwatch',
                                               self.connection.region)
            self._cloudwatch_connection = conn
        return self._cloudwatch_connection

    def describe_all(self):
        """ Describe all tables in the connected region """
        tables = self.connection.list_tables()
        descs = []
        for tablename in tables:
            descs.append(self.describe(tablename, True))
        return descs

    def _get_metric(self, metric, tablename, index_name=None):
        """ Fetch a read/write capacity metric """
        end = time.time()
        begin = end - 20 * 60  # 20 minute window
        dimensions = [{'Name': 'TableName', 'Value': tablename}]
        if index_name is not None:
            dimensions.append({'Name': 'GlobalSecondaryIndexName',
                               'Value': index_name})
        data = self.cloudwatch_connection.get_metric_statistics(
            Period=60,
            StartTime=begin,
            EndTime=end,
            MetricName=metric,
            Namespace='AWS/DynamoDB',
            Statistics=['Average'],
            Dimensions=dimensions,
        )
        points = data['Datapoints']
        if len(points) == 0:
            return 0
        else:
            points.sort(key=lambda r: r['Timestamp'])
            return points[-1]['Average']

    def get_capacity(self, tablename, index_name=None):
        """ Get the consumed read/write capacity """
        # If we're connected to a DynamoDB Local instance, don't connect to the
        # actual cloudwatch endpoint
        if self.connection.region == 'local':
            return 0, 0
        # Gracefully fail if we get exceptions from CloudWatch
        try:
            return (
                self._get_metric('ConsumedReadCapacityUnits', tablename,
                                 index_name),
                self._get_metric('ConsumedWriteCapacityUnits', tablename,
                                 index_name),
            )
        except ClientError:
            return 0, 0

    def describe(self, tablename, refresh=False, metrics=False):
        """ Get the :class:`.TableMeta` for a table """
        if refresh or tablename not in self.cached_descriptions:
            desc = self.connection.describe_table(tablename)
            if desc is None:
                return None
            table = TableMeta.from_description(desc)
            self.cached_descriptions[tablename] = table
            if metrics:
                read, write = self.get_capacity(tablename)
                table.consumed_capacity['__table__'] = {
                    'read': read,
                    'write': write,
                }
                for index_name in table.global_indexes:
                    read, write = self.get_capacity(tablename, index_name)
                    table.consumed_capacity[index_name] = {
                        'read': read,
                        'write': write,
                    }

        return self.cached_descriptions[tablename]

    def execute(self, commands, pformat=False):
        """
        Parse and run a DQL string

        Parameters
        ----------
        commands : str
            The DQL command string
        pformat : bool
            Pretty-format the return value. (e.g. 4 -> 'Updated 4 items')

        """
        tree = parser.parseString(commands)
        for statement in tree:
            result = self._run(statement)
        if pformat:
            return pretty_format(tree[-1], result)
        return result

    def _run(self, tree):
        """ Run a query from a parse tree """
        if tree.action == 'SELECT':
            return self._select(tree, self.allow_select_scan)
        elif tree.action == 'SCAN':
            return self._scan(tree)
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

    def _build_query(self, table, tree, visitor):
        """ Build a scan/query from a statement """
        kwargs = {}
        index = None
        if tree.using:
            index_name = kwargs['index'] = tree.using[1]
            index = table.get_index(index_name)
        if tree.where:
            constraints = ConstraintExpression.from_where(tree.where)
            possible_hash = constraints.possible_hash_fields()
            possible_range = constraints.possible_range_fields()
            if index is None:
                # See if we can find an index to query on
                indexes = table.get_matching_indexes(possible_hash,
                                                     possible_range)
                if len(indexes) == 0:
                    action = 'scan'
                    kwargs['filter'] = constraints.build(visitor)
                    kwargs['expr_values'] = visitor.expression_values
                    if visitor.attribute_names:
                        kwargs['alias'] = visitor.attribute_names
                elif len(indexes) == 1:
                    action = 'query'
                    add_query_kwargs(kwargs, visitor, constraints, indexes[0])
                else:
                    names = ', '.join([index.name for index in indexes])
                    raise SyntaxError("No index specified with USING <index>, "
                                      "but multiple possibilities for query: "
                                      "%s" % names)
            else:
                if index.hash_key in possible_hash:
                    action = 'query'
                    add_query_kwargs(kwargs, visitor, constraints, index)
                else:
                    action = 'scan'
                    if not index.scannable:
                        raise SyntaxError("Cannot scan local index %r" %
                                          index_name)
                    kwargs['filter'] = constraints.build(visitor)
                    kwargs['expr_values'] = visitor.expression_values
                    if visitor.attribute_names:
                        kwargs['alias'] = visitor.attribute_names
        else:
            action = 'scan'
        return [action, kwargs]

    def _iter_where_in(self, tree):
        """ Iterate over the KEYS IN and generate primary keys """
        desc = self.describe(tree.table)
        for keypair in tree.keys_in:
            yield desc.primary_key(*map(resolve, keypair))

    def _select(self, tree, allow_select_scan):
        """ Run a SELECT statement """
        tablename = tree.table
        desc = self.describe(tablename)
        kwargs = {}
        if tree.consistent:
            kwargs['consistent'] = True

        visitor = Visitor(self.reserved_words)
        attr_list = tree.attrs.asList()
        if attr_list == ['COUNT(*)']:
            kwargs['select'] = 'COUNT'
        elif attr_list != ['*']:
            kwargs['attributes'] = [visitor.get_field(a) for a in tree.attrs.asList()]

        if tree.keys_in:
            if tree.limit:
                raise SyntaxError("Cannot use LIMIT with KEYS IN")
            elif tree.using:
                raise SyntaxError("Cannot use USING with KEYS IN")
            elif tree.order:
                raise SyntaxError("Cannot use DESC/ASC with KEYS IN")
            elif tree.where:
                raise SyntaxError("Cannot use WHERE with KEYS IN")
            keys = list(self._iter_where_in(tree))
            return self.connection.batch_get(tablename, keys=keys, **kwargs)

        if tree.limit:
            kwargs['limit'] = resolve(tree.limit[1])

        (action, query_kwargs) = self._build_query(desc, tree, visitor)
        if action == 'scan' and not allow_select_scan:
            raise SyntaxError(
                "No index found for query. Please use a SCAN query, or "
                "set allow_select_scan=True\nopt allow_select_scan true")
        if tree.order:
            if action == 'scan':
                raise SyntaxError("No index found for query, "
                                  "cannot use ASC or DESC")
            kwargs['desc'] = tree.order == 'DESC'

        kwargs.update(query_kwargs)
        if visitor.expression_values:
            kwargs['expr_values'] = visitor.expression_values
        if visitor.attribute_names:
            kwargs['alias'] = visitor.attribute_names
        method = getattr(self.connection, action + '2')
        ret = method(tablename, **kwargs)
        if kwargs.get('select') == 'COUNT':
            return Count.from_response(ret)
        return ret

    def _scan(self, tree):
        """ Run a SCAN statement """
        return self._select(tree, True)

    def _query_and_op(self, tree, table, method_name, method_kwargs):
        """ Query the table and perform an operation on each item """
        if tree.keys_in:
            if tree.using:
                raise SyntaxError("Cannot use USING with KEYS IN")
            keys = self._iter_where_in(tree)
        else:
            visitor = Visitor(self.reserved_words)
            (action, kwargs) = self._build_query(table, tree,
                                                 visitor)
            attrs = [visitor.get_field(table.hash_key.name)]
            if table.range_key is not None:
                attrs.append(visitor.get_field(table.range_key.name))
            kwargs['attributes'] = attrs
            if visitor.expression_values:
                kwargs['expr_values'] = visitor.expression_values
            if visitor.attribute_names:
                kwargs['alias'] = visitor.attribute_names

            method = getattr(self.connection, action + '2')
            keys = method(table.name, **kwargs)

        count = 0
        result = []
        for key in keys:
            method = getattr(self.connection, method_name)
            try:
                ret = method(table.name, key, **method_kwargs)
            except CheckFailed:
                continue
            count += 1
            if ret:
                result.append(ret)
        if result:
            return result
        else:
            return count

    def _delete(self, tree):
        """ Run a DELETE statement """
        tablename = tree.table
        table = self.describe(tablename)
        kwargs = {}
        visitor = Visitor(self.reserved_words)
        if tree.where:
            constraints = ConstraintExpression.from_where(tree.where)
            kwargs['condition'] = constraints.build(visitor)
        kwargs['expr_values'] = visitor.expression_values
        if visitor.attribute_names:
            kwargs['alias'] = visitor.attribute_names
        return self._query_and_op(tree, table, 'delete_item2', kwargs)

    def _update(self, tree):
        """ Run an UPDATE statement """
        tablename = tree.table
        table = self.describe(tablename)
        kwargs = {}

        if tree.returns:
            kwargs['returns'] = '_'.join(tree.returns)
        else:
            kwargs['returns'] = 'NONE'

        visitor = Visitor(self.reserved_words)
        updates = UpdateExpression.from_update(tree.update)
        kwargs['expression'] = updates.build(visitor)
        if tree.where:
            constraints = ConstraintExpression.from_where(tree.where)
            kwargs['condition'] = constraints.build(visitor)
        kwargs['expr_values'] = visitor.expression_values
        if visitor.attribute_names:
            kwargs['alias'] = visitor.attribute_names
        return self._query_and_op(tree, table, 'update_item2', kwargs)

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
                        kwargs['includes'] = [resolve(v) for v in index.include_vars]
                    index_name = resolve(index[1])
                    field = DynamoKey(name, data_type=TYPES[type_])
                    idx = factory(index_name, field, **kwargs)
                    indexes.append(idx)
            else:
                field = DynamoKey(name, data_type=TYPES[type_])
            attrs[field.name] = field

        for gindex in tree.global_indexes:
            global_indexes.append(self._parse_global_index(gindex, attrs))

        throughput = None
        if tree.throughput:
            throughput = Throughput(*map(resolve, tree.throughput))

        try:
            self.connection.create_table(
                tablename, hash_key, range_key, indexes=indexes,
                global_indexes=global_indexes, throughput=throughput)
        except DynamoDBError as e:
            if e.kwargs['Code'] == 'ResourceInUseException' or tree.not_exists:
                return False
            raise
        return True

    def _parse_global_index(self, clause, attrs):
        """ Parse a global index clause and return a GlobalIndex """
        index_type, name = clause[:2]
        name = resolve(name)

        def get_key(field, data_type=None):
            """ Get or set the DynamoKey from the field name """
            if field in attrs:
                key = attrs[field]
                if data_type is not None:
                    if TYPES[data_type] != key.data_type:
                        raise SyntaxError(
                            "Key %r %s already declared with type %s" %
                            field, data_type, key.data_type)
            else:
                if data_type is None:
                    raise SyntaxError("Missing data type for %r" % field)
                key = DynamoKey(field, data_type=TYPES[data_type])
                attrs[field] = key
            return key
        g_hash_key = get_key(*clause.hash_key)
        g_range_key = None
        # For some reason I can't get the throughput section to have a name
        # Use an index instead
        tp_index = 3
        if clause.range_key:
            tp_index += 1
            g_range_key = get_key(*clause.range_key)
        if clause.include_vars:
            tp_index += 1
        kwargs = {}
        if tp_index < len(clause):
            throughput = clause[tp_index]
            kwargs['throughput'] = Throughput(*map(resolve, throughput))
        index_type = clause.index_type[0]
        if index_type in ('ALL', 'INDEX'):
            factory = GlobalIndex.all
        elif index_type == 'KEYS':
            factory = GlobalIndex.keys
        elif index_type == 'INCLUDE':
            factory = GlobalIndex.include
            if not clause.include_vars:
                raise SyntaxError("Include index %r missing include fields" %
                                  name)
            kwargs['includes'] = [resolve(v) for v in clause.include_vars]
        return factory(name, g_hash_key, g_range_key, **kwargs)

    def _insert(self, tree):
        """ Run an INSERT statement """
        tablename = tree.table
        count = 0
        with self.connection.batch_write(tablename) as batch:
            for item in iter_insert_items(tree):
                batch.put(item)
                count += 1
        return count

    def _drop(self, tree):
        """ Run a DROP statement """
        tablename = tree.table
        try:
            self.connection.delete_table(tablename)
        except DynamoDBError as e:
            if e.kwargs['Code'] == 'ResourceNotFoundException' and tree.exists:
                return False
            raise
        return True

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

    def _update_throughput(self, tablename, read, write, index=None, wait=True):
        """ Update the throughput on a table or index """
        def get_desc():
            """ Get the table or global index description """
            desc = self.describe(tablename, refresh=True)
            if index is not None:
                return desc.global_indexes[index]
            return desc
        desc = get_desc()

        def num_or_star(value):
            """ Convert * to 0, otherwise resolve a number """
            return 0 if value == '*' else resolve(value)
        read = num_or_star(read)
        write = num_or_star(write)
        if read <= 0:
            read = desc.read_throughput
        if write <= 0:
            write = desc.write_throughput
        self._set_throughput(tablename, read, write, index)
        desc = get_desc()
        while desc.status == 'UPDATING':  # pragma: no cover
            time.sleep(5)
            desc = get_desc()

    def _alter(self, tree):
        """ Run an ALTER statement """
        if tree.throughput:
            [read, write] = tree.throughput
            index = None
            if tree.index:
                index = tree.index
            self._update_throughput(tree.table, read, write, index)
        elif tree.drop_index:
            updates = [IndexUpdate.delete(tree.drop_index[0])]
            self.connection.update_table(tree.table, index_updates=updates)
        elif tree.create_index:
            # GlobalIndex
            attrs = {}
            index = self._parse_global_index(tree.create_index, attrs)
            updates = [IndexUpdate.create(index)]
            self.connection.update_table(tree.table, index_updates=updates)
        else:
            raise SyntaxError("No alter command found")

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

    def __init__(self, connection=None):
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

    def execute(self, fragment, pformat=True):
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
            return super(FragmentEngine, self).execute(self.last_query,
                                                       pformat)
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
