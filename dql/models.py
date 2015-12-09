""" Data containers """
from __future__ import unicode_literals

import six
from decimal import Decimal
from dynamo3 import TYPES_REV
from dynamo3.fields import snake_to_camel


def format_throughput(available, used=None):
    """ Format the read/write throughput for display """
    if used is None:
        return str(available)
    percent = float(used) / available
    return '{0:.0f}/{1:.0f} ({2:.0%})'.format(used, available, percent)


@six.python_2_unicode_compatible
class QueryIndex(object):

    """
    A representation of global/local indexes that used during query building.

    When building queries, we need to detect if the constraints are sufficient
    to perform a query or if they can only do a scan. This simple container
    class was specifically create to make that logic simpler.

    """

    def __init__(self, name, is_global, hash_key, range_key, attributes=None):
        self.name = name
        self.is_global = is_global
        self.hash_key = hash_key
        self.range_key = range_key
        self.attributes = attributes

    def projects_all_attributes(self, attrs):
        """ Return True if the index projects all the attributes """
        if self.attributes is None:
            return True
        # If attrs is None we are checking if the index projects ALL
        # attributes, and the answer is "no"
        if not attrs:
            return False
        for attr in attrs:
            if attr not in self.attributes:
                return False
        return True

    @property
    def scannable(self):
        """ Only global indexes can be scanned """
        return self.is_global

    @classmethod
    def from_table_index(cls, table, index):
        """ Factory method """
        attributes = None
        if index.range_key is None:
            range_key = None
        else:
            range_key = index.range_key.name
        if hasattr(index, 'hash_key'):
            is_global = True
            hash_key = index.hash_key.name
        else:
            hash_key = table.hash_key.name
            is_global = False
        if index.projection_type in ('KEYS_ONLY', 'INCLUDE'):
            attributes = set([table.hash_key.name])
            if table.range_key is not None:
                attributes.add(table.range_key.name)
            if getattr(index, 'hash_key', None) is not None:
                attributes.add(index.hash_key.name)
            if index.range_key is not None:
                attributes.add(index.range_key.name)
            if index.include_fields is not None:
                attributes.update(index.include_fields)
        return cls(index.name, is_global, hash_key, range_key, attributes)

    def __repr__(self):
        return str(self)

    def __str__(self):
        if self.range_key is None:
            return "QueryIndex(%r, %s)" % (self.name, self.hash_key)
        else:
            return "QueryIndex(%r, %s, %s)" % (self.name, self.hash_key,
                                               self.range_key)


class TableField(object):

    """
    A DynamoDB table attribute

    Parameters
    ----------
    name : str
    data_type : str
        The type of object (e.g. 'STRING', 'NUMBER', etc)
    key_type : str, optional
        The type of key (e.g. 'RANGE', 'HASH', 'INDEX')
    index_name : str, optional
        If the key_type is 'INDEX', this will be the name of the index that
        uses the field as a range key.

    """

    def __init__(self, name, data_type, key_type=None):
        self.name = name
        self.data_type = data_type
        self.key_type = key_type

    @property
    def schema(self):
        """ The DQL syntax for creating this item """
        if self.key_type is None:
            return "%s %s" % (self.name, self.data_type)
        else:
            return "%s %s %s KEY" % (self.name, self.data_type, self.key_type)

    def to_index(self, index_type, index_name, includes=None):
        """ Create an index field from this field """
        return IndexField(self.name, self.data_type, index_type, index_name,
                          includes)

    def __repr__(self):
        base = "TableField('%s', '%s'" % (self.name, self.data_type)
        if self.key_type is None:
            return base + ')'
        base += ", '%s')" % self.key_type
        return base + ')'

    def __str__(self):
        if self.key_type is None:
            return "%s %s" % (self.name, self.data_type)
        else:
            return "%s %s %s KEY" % (self.name, self.data_type, self.key_type)

    def __hash__(self):
        return hash(self.name)

    def __eq__(self, other):
        return (self.name == other.name and
                self.data_type == other.data_type and
                self.key_type == other.key_type)

    def __ne__(self, other):
        return not self.__eq__(other)


class IndexField(TableField):

    """ A TableField that is also part of a Local Secondary Index """

    def __init__(self, name, data_type, index_type, index_name, includes=None):
        super(IndexField, self).__init__(name, data_type, 'INDEX')
        if index_type == 'KEYS_ONLY':
            self.index_type = 'KEYS'
        else:
            self.index_type = index_type
        self.index_name = index_name
        self.includes = includes

    @property
    def schema(self):
        """ The DQL syntax for creating this item """
        schema = "%s %s %s %s('%s'" % (self.name, self.data_type,
                                       self.index_type, self.key_type,
                                       self.index_name)
        if self.includes is not None:
            schema += ', ['
            schema += ', '.join(("'%s'" % i for i in self.includes))
            schema += ']'
        return schema + ')'

    def __repr__(self):
        base = ("IndexField('%s', '%s', '%s', '%s'" %
                (self.name, self.data_type, self.index_type, self.index_name))
        if self.includes is None:
            return base + ')'
        else:
            return base + ", %s)" % (self.includes,)

    def __eq__(self, other):
        return (super(IndexField, self).__eq__(other) and
                self.index_type == other.index_type and
                self.index_name == other.index_name and
                self.includes == other.includes)


class GlobalIndex(object):

    """ Container for global index data """

    def __init__(self, name, index_type, status, hash_key, range_key,
                 read_throughput, write_throughput, size,
                 includes=None, description=None):
        self.name = name
        if index_type == 'KEYS_ONLY':
            self.index_type = 'KEYS'
        else:
            self.index_type = index_type
        self.status = status
        self.size = size
        self.hash_key = hash_key
        self.range_key = range_key
        self.read_throughput = read_throughput
        self.write_throughput = write_throughput
        self.includes = includes
        self.description = description or {}

    @classmethod
    def from_description(cls, description, attrs):
        """ Create an object from a dynamo3 response """
        hash_key = None
        range_key = None
        index_type = description['Projection']['ProjectionType']
        includes = description['Projection'].get('NonKeyAttributes')
        for data in description['KeySchema']:
            name = data['AttributeName']
            if name not in attrs:
                continue
            key_type = data['KeyType']
            if key_type == 'HASH':
                hash_key = TableField(name, attrs[name].data_type, key_type)
            elif key_type == 'RANGE':
                range_key = TableField(name, attrs[name].data_type, key_type)
        throughput = description['ProvisionedThroughput']
        return cls(description['IndexName'], index_type,
                   description['IndexStatus'], hash_key, range_key,
                   throughput['ReadCapacityUnits'],
                   throughput['WriteCapacityUnits'],
                   description.get('IndexSizeBytes', 0), includes, description)

    def __getattr__(self, name):
        camel_name = snake_to_camel(name)
        if camel_name in self.description:
            return self.description[camel_name]
        return super(GlobalIndex, self).__getattribute__(name)

    def __repr__(self):
        return ("GlobalIndex('%s', '%s', '%s', %s, %s, %s, %s, %s)" %
                (self.name, self.index_type, self.status, self.hash_key,
                 self.range_key, self.read_throughput, self.write_throughput,
                 self.includes))

    def pformat(self, consumed_capacity=None):
        """ Pretty format for insertion into table pformat """
        consumed_capacity = consumed_capacity or {}
        lines = []
        parts = ['GLOBAL', self.index_type, 'INDEX', self.name]
        if self.status != 'ACTIVE':
            parts.insert(0, "[%s]" % self.status)
        lines.append(' '.join(parts))
        lines.append('  items: {0:,} ({1:,} bytes)'.format(self.item_count,
                                                           self.size))
        read = 'Read: ' + format_throughput(self.read_throughput,
                                            consumed_capacity.get('read'))
        write = 'Write: ' + format_throughput(self.write_throughput,
                                              consumed_capacity.get('write'))
        lines.append('  ' + read + '  ' + write)
        lines.append('  ' + self.hash_key.schema)
        if self.range_key is not None:
            lines.append('  ' + self.range_key.schema)

        if self.includes is not None:
            keys = '[%s]' % ', '.join(("'%s'" % i for i in self.includes))
            lines.append("  Projection: %s" % keys)
        return '\n'.join(lines)

    @property
    def schema(self):
        """ The DQL fragment for constructing this index """
        if self.status == 'DELETING':
            return ''
        parts = ['GLOBAL', self.index_type, 'INDEX']
        parts.append("('%s', %s," % (self.name, self.hash_key.name))
        if self.range_key:
            parts.append("%s," % self.range_key.name)
        if self.includes:
            parts.append("[%s]," % ', '.join(("'%s'" % i for i in
                                              self.includes)))

        parts.append("THROUGHPUT (%d, %d))" % (self.read_throughput,
                                               self.write_throughput))
        return ' '.join(parts)

    def __hash__(self):
        return hash(self.name)

    def __eq__(self, other):
        """ Check if schemas are equivalent """
        return (self.name == other.name and
                self.index_type == other.index_type and
                self.hash_key == other.hash_key and
                self.range_key == other.range_key and
                self.read_throughput == other.read_throughput and
                self.write_throughput == other.write_throughput and
                self.includes == other.includes)

    def __ne__(self, other):
        return not self.__eq__(other)


class TableMeta(object):

    """
    Container for table metadata

    Parameters
    ----------
    name : str
    status : str
    attrs : dict
        Mapping of attribute name to :class:`.TableField`
    global_indexes : dict
        Mapping of hash key to :class:`.GlobalIndex`
    read_throughput : int
    write_throughput : int
    decreases_today : int
    size : int
        Size of the table in bytes
    item_count : int
        Number of items in the table

    """

    def __init__(self, table, attrs, global_indexes, read_throughput,
                 write_throughput, decreases_today, size):
        self._table = table
        self.attrs = attrs
        self.size = size
        self.global_indexes = global_indexes
        self.read_throughput = read_throughput
        self.write_throughput = write_throughput
        self.decreases_today = decreases_today
        self.consumed_capacity = {}
        self.hash_key = None
        self.range_key = None
        for field in six.itervalues(attrs):
            if field.key_type == 'HASH':
                self.hash_key = field
            elif field.key_type == 'RANGE':
                self.range_key = field

    def iter_query_indexes(self):
        """
        Iterator that constructs :class:`~dql.models.QueryIndex` for all global
        and local indexes, and a special one for the default table hash & range
        key with the name 'TABLE'

        """
        if self._table.range_key is None:
            range_key = None
        else:
            range_key = self._table.range_key.name
        yield QueryIndex('TABLE', True, self._table.hash_key.name, range_key)
        for index in self._table.indexes:
            yield QueryIndex.from_table_index(self._table, index)
        for index in self._table.global_indexes:
            yield QueryIndex.from_table_index(self._table, index)

    def get_matching_indexes(self, possible_hash, possible_range):
        """
        Get all indexes that could be queried on using a set of keys.

        If any indexes match both hash AND range keys, indexes that only match
        the hash key will be excluded from the result.

        Parameters
        ----------
        possible_hash : set
            The names of fields that could be used as the hash key
        possible_range : set
            The names of fields that could be used as the range key

        """
        matches = [index for index in self.iter_query_indexes()
                   if index.hash_key in possible_hash]
        range_matches = [index for index in matches
                         if index.range_key in possible_range]
        if range_matches:
            return range_matches
        return matches

    def get_index(self, index_name):
        """ Get a specific index by name """
        try:
            return self.get_indexes()[index_name]
        except KeyError:
            raise SyntaxError("Unknown index %r" % index_name)

    def get_indexes(self):
        """ Get a dict of index names to index """
        ret = {}
        for index in self.iter_query_indexes():
            ret[index.name] = index
        return ret

    @classmethod
    def from_description(cls, table):
        """ Factory method that uses the dynamo3 'describe' return value """
        throughput = table.provisioned_throughput
        attrs = {}
        for data in getattr(table, 'attribute_definitions', []):
            field = TableField(data['AttributeName'],
                               TYPES_REV[data['AttributeType']])
            attrs[field.name] = field
        for data in getattr(table, 'key_schema', []):
            name = data['AttributeName']
            attrs[name].key_type = data['KeyType']
        for index in getattr(table, 'local_secondary_indexes', []):
            for data in index['KeySchema']:
                if data['KeyType'] == 'RANGE':
                    name = data['AttributeName']
                    index_type = index['Projection']['ProjectionType']
                    includes = index['Projection'].get('NonKeyAttributes')
                    attrs[name] = attrs[name].to_index(index_type,
                                                       index['IndexName'],
                                                       includes)
                    break
        global_indexes = {}
        for index in getattr(table, 'global_secondary_indexes', []):
            idx = GlobalIndex.from_description(index, attrs)
            global_indexes[idx.name] = idx
        return cls(table, attrs,
                   global_indexes, throughput['ReadCapacityUnits'],
                   throughput['WriteCapacityUnits'],
                   throughput['NumberOfDecreasesToday'],
                   table.table_size_bytes)

    def __getattr__(self, name):
        return getattr(self._table, name)

    @property
    def primary_key_attributes(self):
        """ Get the names of the primary key attributes as a tuple """
        if self.range_key is None:
            return (self.hash_key.name,)
        else:
            return (self.hash_key.name, self.range_key.name)

    def primary_key_tuple(self, item):
        """ Get the primary key tuple from an item """
        if self.range_key is None:
            return (item[self.hash_key.name],)
        else:
            return (item[self.hash_key.name], item[self.range_key.name])

    def primary_key(self, hkey, rkey=None):
        """
        Construct a primary key dictionary

        You can either pass in a (hash_key[, range_key]) as the arguments, or
        you may pass in an Item itself

        """
        if isinstance(hkey, dict):
            def decode(val):
                """ Convert Decimals back to primitives """
                if isinstance(val, Decimal):
                    return float(val)
                return val
            pkey = {
                self.hash_key.name: decode(hkey[self.hash_key.name])
            }
            if self.range_key is not None:
                pkey[self.range_key.name] = decode(hkey[self.range_key.name])
            return pkey
        else:
            pkey = {
                self.hash_key.name: hkey
            }
            if self.range_key is not None:
                if rkey is None:
                    raise ValueError("Range key is missing!")
                pkey[self.range_key.name] = rkey
            return pkey

    @property
    def total_read_throughput(self):
        """ Combined read throughput of table and global indexes """
        total = self.read_throughput
        for index in six.itervalues(self.global_indexes):
            total += index.read_throughput
        return total

    @property
    def total_write_throughput(self):
        """ Combined write throughput of table and global indexes """
        total = self.write_throughput
        for index in six.itervalues(self.global_indexes):
            total += index.write_throughput
        return total

    def __len__(self):
        return self.item_count

    def __repr__(self):
        return 'TableMeta(%s)' % self.name

    def __str__(self):
        return self.name

    def __hash__(self):
        return hash(self.name)

    def __eq__(self, other):
        """ Check if schemas are equivalent """
        return (self.name == other.name and
                self.attrs == other.attrs and
                self.range_key == other.range_key and
                self.global_indexes == other.global_indexes and
                self.read_throughput == other.read_throughput and
                self.write_throughput == other.write_throughput)

    def __ne__(self, other):
        return not self.__eq__(other)

    @property
    def schema(self):
        """ The DQL query that will construct this table's schema """
        attrs = self.attrs.copy()
        parts = ['CREATE', 'TABLE', self.name, '(%s,' % self.hash_key.schema]
        del attrs[self.hash_key.name]
        if self.range_key:
            parts.append(self.range_key.schema + ',')
            del attrs[self.range_key.name]
        if attrs:
            attr_def = ', '.join([attr.schema for attr in
                                  six.itervalues(attrs)])
            parts.append(attr_def + ',')

        parts.append("THROUGHPUT (%d, %d))" % (self.read_throughput,
                                               self.write_throughput))
        parts.extend([g.schema for g in six.itervalues(self.global_indexes)])
        return ' '.join(parts) + ';'

    def pformat(self):
        """ Pretty string format """
        lines = []
        lines.append(("%s (%s)" % (self.name, self.status)).center(50, '-'))
        lines.append('items: {0:,} ({1:,} bytes)'.format(self.item_count,
                                                         self.size))
        cap = self.consumed_capacity.get('__table__', {})
        read = 'Read: ' + format_throughput(self.read_throughput,
                                            cap.get('read'))
        write = 'Write: ' + format_throughput(self.write_throughput,
                                              cap.get('write'))
        lines.append(read + '  ' + write)
        if self.decreases_today > 0:
            lines.append('decreases today: %d' % self.decreases_today)

        if self.range_key is None:
            lines.append(str(self.hash_key))
        else:
            lines.append("%s, %s" % (self.hash_key, self.range_key))

        for field in six.itervalues(self.attrs):
            if field.key_type == 'INDEX':
                lines.append(str(field))

        for index_name, gindex in six.iteritems(self.global_indexes):
            cap = self.consumed_capacity.get(index_name)
            lines.append(gindex.pformat(cap))

        return '\n'.join(lines)
