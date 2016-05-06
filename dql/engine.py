""" Execution engine """
import gzip
import os
import time

import botocore
import csv
import json
import pickle
import logging
import six
from base64 import b64encode
from botocore.exceptions import ClientError
from decimal import Decimal
from dynamo3 import (TYPES, DynamoDBConnection, DynamoKey, LocalIndex,
                     GlobalIndex, DynamoDBError, Throughput, CheckFailed,
                     IndexUpdate, Limit, RateLimit, Capacity, Binary)
from dynamo3.constants import RESERVED_WORDS
from pprint import pformat
from pyparsing import ParseException

from .expressions import (ConstraintExpression, UpdateExpression, Visitor,
                          SelectionExpression)
from .grammar import parser, line_parser
from .models import TableMeta
from .util import resolve, unwrap, plural

LOG = logging.getLogger(__name__)


def default(value):
    """ Default encoder for JSON """
    if isinstance(value, Decimal):
        try:
            return int(value)
        except ValueError:
            return float(value)
    elif isinstance(value, set):
        return list(value)
    elif isinstance(value, Binary):
        return b64encode(value.value)
    raise TypeError("Cannot encode %s value %r" % (type(value), value))


class ExplainSignal(Exception):
    """ Thrown to stop a query when we're doing an EXPLAIN """
    pass


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


class Engine(object):

    """
    DQL execution engine

    Parameters
    ----------
    connection : :class:`~dynamo3.DynamoDBConnection`, optional
        If not present, you will need to call :meth:`.Engine.connect`

    Attributes
    ----------
    caution_callback : callable, optional
        Called to prompt user when a potentially dangerous action is about to
        occur.

    """

    def __init__(self, connection=None):
        self._connection = None
        self.connection = connection
        self.cached_descriptions = {}
        self._cloudwatch_connection = None
        self.allow_select_scan = False
        self.reserved_words = RESERVED_WORDS
        self._session = None
        self.consumed_capacities = []
        self._call_list = []
        self._explaining = False
        self._analyzing = False
        self._query_rate_limit = None
        self.rate_limit = None
        self._encoder = json.JSONEncoder(separators=(',', ':'),
                                         default=default)
        self.caution_callback = None

    def connect(self, *args, **kwargs):
        """ Proxy to DynamoDBConnection.connect. """
        self.connection = DynamoDBConnection.connect(*args, **kwargs)
        self._session = kwargs.get('session')
        if self._session is None:
            self._session = botocore.session.get_session()

    @property
    def region(self):
        """ Get the connected dynamo region or host """
        return self._connection.region

    @property
    def connection(self):
        """ Get the dynamo connection """
        return self._connection

    @connection.setter
    def connection(self, connection):
        """ Change the dynamo connection """
        if connection is not None:
            connection.subscribe('capacity', self._on_capacity_data)
            connection.default_return_capacity = True
        if self._connection is not None:
            connection.unsubscribe('capacity', self._on_capacity_data)
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

    def _format_explain(self):
        """ Format the results of an EXPLAIN """
        lines = []
        for (command, kwargs) in self._call_list:
            lines.append(command + ' ' + pformat(kwargs))
        return '\n'.join(lines)

    def _pretty_format(self, statement, result):
        """ Format the return value of a query for humans """
        if result is None:
            return 'Success'
        ret = result
        if statement.action in ('SELECT', 'SCAN'):
            if statement.save_file:
                filename = statement.save_file[0]
                if filename[0] in ['"', "'"]:
                    filename = unwrap(filename)
                ret = "Saved %d record%s to %s" % (result, plural(result),
                                                   filename)
            elif isinstance(result, six.integer_types):
                if result == result.scanned_count:
                    ret = "%d" % result
                else:
                    ret = "%d (scanned count: %d)" % (result,
                                                      result.scanned_count)
        elif statement.action == 'UPDATE':
            if isinstance(result, six.integer_types):
                ret = "Updated %d item%s" % (result, plural(result))
        elif statement.action == 'DELETE':
            ret = "Deleted %d item%s" % (result, plural(result))
        elif statement.action == 'CREATE':
            if result:
                ret = "Created table %r" % statement.table
            else:
                ret = "Table %r already exists" % statement.table
        elif statement.action == 'INSERT':
            ret = "Inserted %d item%s" % (result, plural(result))
        elif statement.action == 'DROP':
            if result:
                ret = "Dropped table %r" % statement.table
            else:
                ret = "Table %r does not exist" % statement.table
        elif statement.action == 'ANALYZE':
            ret = self._pretty_format(statement[1], result)
        elif statement.action == 'LOAD':
            ret = "Loaded %d item%s" % (result, plural(result))
        return ret

    def describe_all(self, refresh=True):
        """ Describe all tables in the connected region """
        tables = self.connection.list_tables()
        descs = []
        for tablename in tables:
            descs.append(self.describe(tablename, refresh))
        return descs

    def _get_metric(self, metric, tablename, index_name=None):
        """ Fetch a read/write capacity metric """
        end = time.time()
        begin = end - 3 * 60  # 3 minute window
        dimensions = [{'Name': 'TableName', 'Value': tablename}]
        if index_name is not None:
            dimensions.append({'Name': 'GlobalSecondaryIndexName',
                               'Value': index_name})
        period = 60
        data = self.cloudwatch_connection.get_metric_statistics(
            Period=period,
            StartTime=begin,
            EndTime=end,
            MetricName=metric,
            Namespace='AWS/DynamoDB',
            Statistics=['Sum'],
            Dimensions=dimensions,
        )
        points = data['Datapoints']
        if len(points) == 0:
            return 0
        else:
            points.sort(key=lambda r: r['Timestamp'])
            return float(points[-1]['Sum']) / period

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

    def describe(self, tablename, refresh=False, metrics=False, require=False):
        """ Get the :class:`.TableMeta` for a table """
        if refresh or tablename not in self.cached_descriptions:
            desc = self.connection.describe_table(tablename)
            if desc is None:
                if require:
                    raise RuntimeError("Table %r not found" % tablename)
                else:
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

    def execute(self, commands, pretty_format=False):
        """
        Parse and run a DQL string

        Parameters
        ----------
        commands : str
            The DQL command string
        pretty_format : bool
            Pretty-format the return value. (e.g. 4 -> 'Updated 4 items')

        """
        tree = parser.parseString(commands)
        self.consumed_capacities = []
        self._analyzing = False
        self._query_rate_limit = None
        for statement in tree:
            try:
                result = self._run(statement)
            except ExplainSignal:
                return self._format_explain()
        if pretty_format:
            return self._pretty_format(tree[-1], result)
        return result

    def _run(self, tree):
        """ Run a query from a parse tree """
        if tree.throttle:
            limiter = self._parse_throttle(tree.table, tree.throttle)
            self._query_rate_limit = limiter
            del tree['throttle']
            return self._run(tree)
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
        elif tree.action == 'LOAD':
            return self._load(tree)
        elif tree.action == 'EXPLAIN':
            return self._explain(tree)
        elif tree.action == 'ANALYZE':
            self._analyzing = True
            self.connection.default_return_capacity = True
            return self._run(tree[1])
        else:
            raise SyntaxError("Unrecognized action '%s'" % tree.action)

    def _parse_throttle(self, tablename, throttle):
        """ Parse a 'throttle' statement and return a RateLimit """
        amount = []
        desc = self.describe(tablename)
        throughputs = [desc.read_throughput, desc.write_throughput]
        for value, throughput in zip(throttle[1:], throughputs):
            if value == '*':
                amount.append(0)
            elif value[-1] == '%':
                amount.append(throughput * float(value[:-1]) / 100.)
            else:
                amount.append(float(value))
        cap = Capacity(*amount)  # pylint: disable=E1120
        return RateLimit(total=cap, callback=self._on_throttle)

    def _on_capacity_data(self, conn, command, kwargs, response, capacity):
        """ Log the received consumed capacity data """
        if self._analyzing:
            self.consumed_capacities.append((command, capacity))
        if self._query_rate_limit is not None:
            self._query_rate_limit.on_capacity(conn, command, kwargs, response,
                                               capacity)
        elif self.rate_limit is not None:
            self.rate_limit.callback = self._on_throttle
            self.rate_limit.on_capacity(conn, command, kwargs, response,
                                        capacity)

    def _on_throttle(self, conn, command, kwargs, response, capacity, seconds):
        """ Print out a message when the query is throttled """
        LOG.info("Throughput limit exceeded during %s. "
                 "Sleeping for %d second%s",
                 command, seconds, plural(seconds))

    def _explain(self, tree):
        """ Set up the engine to do a dry run of a query """
        self._explaining = True
        self._call_list = []
        old_call = self.connection.call

        def fake_call(command, **kwargs):
            """ Replacement for connection.call that logs args """
            if command == 'describe_table':
                return old_call(command, **kwargs)
            self._call_list.append((command, kwargs))
            raise ExplainSignal

        self.connection.call = fake_call
        try:
            ret = self._run(tree[1])
            try:
                list(ret)
            except TypeError:
                pass
        finally:
            self.connection.call = old_call
            self._explaining = False

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
                    kwargs['alias'] = visitor.attribute_names
                elif len(indexes) == 1:
                    index = indexes[0]
                    action = 'query'
                    add_query_kwargs(kwargs, visitor, constraints, index)
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
                    kwargs['alias'] = visitor.attribute_names
        else:
            action = 'scan'
        return [action, kwargs, index]

    def _iter_where_in(self, tree):
        """ Iterate over the KEYS IN and generate primary keys """
        desc = self.describe(tree.table, require=True)
        for keypair in tree.keys_in:
            yield desc.primary_key(*map(resolve, keypair))

    def _select(self, tree, allow_select_scan):
        """ Run a SELECT statement """
        tablename = tree.table
        desc = self.describe(tablename, require=True)
        kwargs = {}
        if tree.consistent:
            kwargs['consistent'] = True

        visitor = Visitor(self.reserved_words)

        selection = SelectionExpression.from_selection(tree.attrs)
        if selection.is_count:
            kwargs['select'] = 'COUNT'

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
            kwargs['attributes'] = selection.build(visitor)
            kwargs['alias'] = visitor.attribute_names
            return self.connection.batch_get(tablename, keys=keys, **kwargs)

        if tree.limit:
            if tree.scan_limit:
                kwargs['limit'] = Limit(scan_limit=resolve(tree.scan_limit[2]),
                                        item_limit=resolve(tree.limit[1]),
                                        strict=True)
            else:
                kwargs['limit'] = Limit(item_limit=resolve(tree.limit[1]),
                                        strict=True)
        elif tree.scan_limit:
            kwargs['limit'] = Limit(scan_limit=resolve(tree.scan_limit[2]))

        (action, query_kwargs, index) = self._build_query(desc, tree, visitor)
        if action == 'scan' and not allow_select_scan:
            raise SyntaxError(
                "No index found for query. Please use a SCAN query, or "
                "set allow_select_scan=True\nopt allow_select_scan true")
        order_by = None
        if tree.order_by:
            order_by = tree.order_by[0]
        reverse = tree.order == 'DESC'
        if tree.order:
            if action == 'scan' and not tree.order_by:
                raise SyntaxError("No index found for query, "
                                  "cannot use ASC or DESC without "
                                  "ORDER BY <field>")
            if action == 'query':
                if order_by is None or order_by == index.range_key:
                    kwargs['desc'] = reverse

        kwargs.update(query_kwargs)

        # This is a special case for when we're querying an index and selecting
        # fields that aren't projected into the index.
        # We will change the query to only fetch the primary keys, and then
        # fill in the selected attributes after the fact.
        fetch_attrs_after = False
        if (index is not None and
                not index.projects_all_attributes(selection.all_fields)):
            kwargs['attributes'] = [visitor.get_field(a) for a in
                                    desc.primary_key_attributes]
            fetch_attrs_after = True
        else:
            kwargs['attributes'] = selection.build(visitor)
        kwargs['expr_values'] = visitor.expression_values
        kwargs['alias'] = visitor.attribute_names

        method = getattr(self.connection, action + '2')
        result = method(tablename, **kwargs)

        # If the queried index didn't project the selected attributes, we need
        # to do a BatchGetItem to fetch all the data.
        if fetch_attrs_after:
            if not isinstance(result, list):
                result = list(result)
            # If no results, no need to batch_get
            if not result:
                return result
            visitor = Visitor(self.reserved_words)
            kwargs = {
                'keys': [desc.primary_key(item) for item in result],
            }
            kwargs['attributes'] = selection.build(visitor)
            kwargs['alias'] = visitor.attribute_names
            result = self.connection.batch_get(tablename, **kwargs)

        def order(items):
            """ Sort the items by the specified keys """
            if order_by is None:
                return items
            if index is None or order_by != index.range_key:
                if not isinstance(items, list):
                    items = list(items)
                items.sort(key=lambda x: x.get(order_by), reverse=reverse)
            return items

        # Save the data to a file
        if tree.save_file:
            if selection.is_count:
                raise Exception("Cannot use count(*) with SAVE")
            count = 0
            result = order(selection.convert(item, True) for item in result)
            filename = tree.save_file[0]
            if filename[0] in ['"', "'"]:
                filename = unwrap(filename)
            # If it's still an iterator, convert to a list so we can iterate
            # multiple times.
            if not isinstance(result, list):
                result = list(result)
            remainder, ext = os.path.splitext(filename)
            if ext.lower() in ['.gz', '.gzip']:
                ext = os.path.splitext(remainder)[1]
                opened = gzip.open(filename, 'wb')
            else:
                opened = open(filename, 'wb')
            if ext.lower() == '.csv':
                if selection.all_keys:
                    headers = selection.all_keys
                else:
                    # Have to do this to get all the headers :(
                    result = list(result)
                    all_headers = set()
                    for item in result:
                        all_headers.update(item.keys())
                    headers = list(all_headers)
                with opened as ofile:
                    writer = csv.DictWriter(ofile, fieldnames=headers,
                                            extrasaction='ignore')
                    writer.writeheader()
                    for item in result:
                        count += 1
                        writer.writerow(item)
            elif ext.lower() == '.json':
                with opened as ofile:
                    for item in result:
                        count += 1
                        ofile.write(self._encoder.encode(item))
                        ofile.write('\n')
            else:
                with opened as ofile:
                    for item in result:
                        count += 1
                        pickle.dump(item, ofile)
            return count
        elif not selection.is_count:
            result = order(selection.convert(item) for item in result)

        return result

    def _scan(self, tree):
        """ Run a SCAN statement """
        return self._select(tree, True)

    def _query_and_op(self, tree, table, method_name, method_kwargs):
        """ Query the table and perform an operation on each item """
        result = []
        if tree.keys_in:
            if tree.using:
                raise SyntaxError("Cannot use USING with KEYS IN")
            keys = self._iter_where_in(tree)
        else:
            visitor = Visitor(self.reserved_words)
            (action, kwargs, _) = self._build_query(table, tree, visitor)
            attrs = [visitor.get_field(table.hash_key.name)]
            if table.range_key is not None:
                attrs.append(visitor.get_field(table.range_key.name))
            kwargs['attributes'] = attrs
            kwargs['expr_values'] = visitor.expression_values
            kwargs['alias'] = visitor.attribute_names
            # If there is no 'where' on this update/delete, check with the
            # caution_callback before proceeding.
            if visitor.expression_values is None and \
                    callable(self.caution_callback) and \
                    not self.caution_callback(method_name):  # pylint: disable=E1102
                return False
            method = getattr(self.connection, action + '2')
            keys = method(table.name, **kwargs)
            if self._explaining:
                try:
                    list(keys)
                except ExplainSignal:
                    keys = [{}]  # pylint: disable=R0204

        method = getattr(self.connection, method_name + '2')
        count = 0
        for key in keys:
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
        table = self.describe(tablename, require=True)
        kwargs = {}
        visitor = Visitor(self.reserved_words)
        if tree.where:
            constraints = ConstraintExpression.from_where(tree.where)
            kwargs['condition'] = constraints.build(visitor)
        kwargs['expr_values'] = visitor.expression_values
        kwargs['alias'] = visitor.attribute_names
        return self._query_and_op(tree, table, 'delete_item', kwargs)

    def _update(self, tree):
        """ Run an UPDATE statement """
        tablename = tree.table
        table = self.describe(tablename, require=True)
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
        kwargs['alias'] = visitor.attribute_names
        return self._query_and_op(tree, table, 'update_item', kwargs)

    def _create(self, tree):
        """ Run a SELECT statement """
        tablename = tree.table
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
            ret = self.connection.create_table(
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
        kwargs = {}
        batch = self.connection.batch_write(tablename, **kwargs)
        with batch:
            for item in iter_insert_items(tree):
                batch.put(item)
                count += 1
        return count

    def _drop(self, tree):
        """ Run a DROP statement """
        tablename = tree.table
        kwargs = {}
        try:
            ret = self.connection.delete_table(tablename, **kwargs)
        except DynamoDBError as e:
            if e.kwargs['Code'] == 'ResourceNotFoundException' and tree.exists:
                return False
            raise
        return True

    def _update_throughput(self, tablename, read, write, index):
        """ Update the throughput on a table or index """
        def get_desc():
            """ Get the table or global index description """
            desc = self.describe(tablename, refresh=True, require=True)
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

        throughput = Throughput(read, write)
        kwargs = {}
        if index:
            kwargs['global_indexes'] = {
                index: throughput,
            }
        else:
            kwargs['throughput'] = throughput
        self.connection.update_table(tablename, **kwargs)
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
            try:
                self.connection.update_table(tree.table,
                                             index_updates=updates)
            except DynamoDBError as e:
                if tree.exists and e.kwargs['Code'] == 'ResourceNotFoundException':
                    pass
                else:
                    raise
        elif tree.create_index:
            # GlobalIndex
            attrs = {}
            index = self._parse_global_index(tree.create_index, attrs)
            updates = [IndexUpdate.create(index)]
            try:
                self.connection.update_table(tree.table,
                                             index_updates=updates)
            except DynamoDBError as e:
                if (tree.not_exists and
                        e.kwargs['Code'] == 'ValidationException' and
                        'already exists' in e.kwargs['Message']):
                    pass
                else:
                    raise
        else:
            raise SyntaxError("No alter command found")

    def _dump(self, tree):
        """ Run a DUMP statement """
        schema = []
        if tree.tables:
            for table in tree.tables:
                desc = self.describe(table, refresh=True, require=True)
                schema.append(desc.schema)
        else:
            for table in self.describe_all():
                schema.append(table.schema)

        return '\n\n'.join(schema)

    def _load(self, tree):
        """ Run a LOAD statement """
        filename = tree.load_file[0]
        if filename[0] in ['"', "'"]:
            filename = unwrap(filename)
        if not os.path.exists(filename):
            raise Exception("No such file %r" % filename)
        batch = self.connection.batch_write(tree.table)
        count = 0
        with batch:
            remainder, ext = os.path.splitext(filename)
            if ext.lower() in ['.gz', '.gzip']:
                ext = os.path.splitext(remainder)[1]
                opened = gzip.open(filename, 'rb')
            else:
                opened = open(filename, 'r')
            with opened as ifile:
                if ext.lower() == '.csv':
                    reader = csv.DictReader(ifile)
                    for row in reader:
                        batch.put(row)
                        count += 1
                elif ext.lower() == '.json':
                    for row in ifile:
                        batch.put(json.loads(row))
                        count += 1
                else:
                    try:
                        while True:
                            batch.put(pickle.load(ifile))
                            count += 1
                    except EOFError:
                        pass
        return count


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

    def execute(self, fragment, pretty_format=True):
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
                                                       pretty_format)
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
