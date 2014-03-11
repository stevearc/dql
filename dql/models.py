""" Data containers """
import six
from decimal import Decimal
from dynamo3 import TYPES_REV


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
                 read_throughput, write_throughput, size, item_count,
                 includes=None):
        self.name = name
        if index_type == 'KEYS_ONLY':
            self.index_type = 'KEYS'
        else:
            self.index_type = index_type
        self.size = size
        self.status = status
        self.item_count = item_count
        self.hash_key = hash_key
        self.range_key = range_key
        self.read_throughput = read_throughput
        self.write_throughput = write_throughput
        self.includes = includes

    @classmethod
    def from_description(cls, description, attrs):
        """ Create an object from a dynamo3 response """
        range_key = None
        index_type = description['Projection']['ProjectionType']
        includes = description['Projection'].get('NonKeyAttributes')
        for data in description['KeySchema']:
            name = data['AttributeName']
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
                   description['IndexSizeBytes'], description['ItemCount'],
                   includes)

    def __repr__(self):
        return ("GlobalIndex('%s', '%s', '%s', %s, %s, %s, %s, %s)" %
                (self.name, self.index_type, self.status, self.hash_key,
                 self.range_key, self.read_throughput, self.write_throughput,
                 self.includes))

    def pformat(self):
        """ Pretty format for insertion into table pformat """
        parts = ['GLOBAL', self.index_type, 'INDEX', self.status, self.name]
        keys = "(%s" % self.hash_key.name
        if self.range_key is not None:
            keys += ", %s" % self.range_key.name
        if self.includes is not None:
            keys += ', [%s]' % ', '.join(("'%s'" % i for i in self.includes))
        keys += ')'

        parts.append(keys)
        parts.extend(['THROUGHPUT', "(%d, %d)" % (self.read_throughput,
                                                  self.write_throughput)])

        return ' '.join(parts)

    @property
    def schema(self):
        """ The DQL fragment for constructing this index """
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
        Mapping of attribute name to :class:`.TableField`s
    global_indexes : dict
        Mapping of hash key to :class:`.GlobalIndex`
    read_throughput : int
    write_throughput : int
    decreases_today : int
    consumed_read_capacity : int
        May be None if unknown
    consumed_write_capacity : int
        May be None if unknown
    size : int
        Size of the table in bytes
    item_count : int
        Number of items in the table

    """

    def __init__(self, name, status, attrs, global_indexes,
                 read_throughput, write_throughput, decreases_today, size,
                 item_count):
        self.name = name
        self.attrs = attrs
        self.size = size
        self.status = status
        self.item_count = item_count
        self.global_indexes = global_indexes
        self.read_throughput = read_throughput
        self.write_throughput = write_throughput
        self.decreases_today = decreases_today
        self.consumed_read_capacity = None
        self.consumed_write_capacity = None
        self.hash_key = None
        self.range_key = None
        for field in six.itervalues(attrs):
            if field.key_type == 'HASH':
                self.hash_key = field
            elif field.key_type == 'RANGE':
                self.range_key = field

    @classmethod
    def from_description(cls, desc):
        """ Factory method that uses the dynamo3 'describe' return value """
        table = desc['Table']
        throughput = table['ProvisionedThroughput']
        attrs = {}
        for data in table.get('AttributeDefinitions', []):
            field = TableField(data['AttributeName'],
                               TYPES_REV[data['AttributeType']])
            attrs[field.name] = field
        for data in table.get('KeySchema', []):
            name = data['AttributeName']
            attrs[name].key_type = data['KeyType']
        for index in table.get('LocalSecondaryIndexes', []):
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
        for index in table.get('GlobalSecondaryIndexes', []):
            idx = GlobalIndex.from_description(index, attrs)
            global_indexes[idx.name] = idx
        return cls(table['TableName'], table['TableStatus'], attrs,
                   global_indexes, throughput['ReadCapacityUnits'],
                   throughput['WriteCapacityUnits'],
                   throughput['NumberOfDecreasesToday'],
                   table['TableSizeBytes'], table['ItemCount'],)

    def primary_key(self, hkey, rkey=None):
        """
        Construct a primary key dictionary

        You can either pass in a (hash_key[, range_key]) as the arguments, or
        you may pass in an Item itself

        """
        if isinstance(hkey, six.string_types):
            pkey = {
                self.hash_key.name: hkey
            }
            if self.range_key is not None:
                if rkey is None:
                    raise ValueError("Range key is missing!")
                pkey[self.range_key.name] = rkey
            return pkey
        else:
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

    @property
    def decreases_remaining(self):
        """ Number of remaining times you may decrease throughput today """
        return 2 - self.decreases_today

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
            attr_def = ', '.join([attr.schema for attr in six.itervalues(attrs)])
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
        lines.append('read/write: %d/%d' % (self.read_throughput,
                                            self.write_throughput))
        if self.consumed_read_capacity is not None:
            read_percent = self.consumed_read_capacity / self.read_throughput
            write_percent = (self.consumed_write_capacity /
                             self.write_throughput)
            lines.append('read/write usage: {0:.1f}/{1:.1f} ({2:.1%}/{3:.1%})'
                         .format(self.consumed_read_capacity,
                                 self.consumed_write_capacity, read_percent,
                                 write_percent))
        lines.append('decreases remaining: %d' % self.decreases_remaining)

        for gindex in six.itervalues(self.global_indexes):
            lines.append(gindex.pformat())

        if self.hash_key is not None:
            lines.append(str(self.hash_key))
        if self.range_key is not None:
            lines.append(str(self.range_key))

        for field in six.itervalues(self.attrs):
            if field.key_type == 'INDEX':
                lines.append(str(field))

        for field in six.itervalues(self.attrs):
            if field.key_type is None:
                lines.append(str(field))

        return '\n'.join(lines)
