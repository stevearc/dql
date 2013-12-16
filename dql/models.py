""" Data containers """
from decimal import Decimal
from boto.dynamodb2.types import (NUMBER, STRING, BINARY, NUMBER_SET,
                                  STRING_SET, BINARY_SET)

TYPES = {
    NUMBER: 'NUMBER',
    STRING: 'STRING',
    BINARY: 'BINARY',
    NUMBER_SET: 'NUMBER_SET',
    STRING_SET: 'STRING_SET',
    BINARY_SET: 'BINARY_SET',
}


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

    def __init__(self, name, data_type, key_type=None, index_name=None):
        self.name = name
        self.data_type = data_type
        self.key_type = key_type
        self.index_name = index_name

    @property
    def schema(self):
        """ The DQL syntax for creating this item """
        if self.key_type is None:
            return "%s %s" % (self.name, self.data_type)
        elif self.index_name is not None:
            return "%s %s %s('%s')" % (self.name, self.data_type,
                                       self.key_type, self.index_name)
        else:
            return "%s %s %s KEY" % (self.name, self.data_type, self.key_type)

    def __repr__(self):
        base = "TableField('%s', '%s'" % (self.name, self.data_type)
        if self.key_type is None:
            return base + ')'
        base += ", '%s'" % self.key_type
        if self.index_name is None:
            return base + ')'
        else:
            return base + ", '%s')" % self.index_name

    def __str__(self):
        if self.key_type is None:
            return "%s %s" % (self.name, self.data_type)
        elif self.index_name is not None:
            return "%s %s %s('%s')" % (self.name, self.data_type,
                                       self.key_type, self.index_name)
        else:
            return "%s %s %s KEY" % (self.name, self.data_type, self.key_type)

    def __hash__(self):
        return hash(self.name)

    def __eq__(self, other):
        return (self.name == other.name and
                self.data_type == other.data_type and
                self.key_type == other.key_type and
                self.index_name == other.index_name)


class GlobalIndex(object):

    """ Container for global index data """

    def __init__(self, name, status, hash_key, range_key,
                 read_throughput, write_throughput, size, item_count):
        self.name = name
        self.size = size
        self.status = status
        self.item_count = item_count
        self.hash_key = hash_key
        self.range_key = range_key
        self.read_throughput = read_throughput
        self.write_throughput = write_throughput

    @classmethod
    def from_description(cls, description, attrs):
        """ Create an object from a boto response """
        range_key = None
        for data in description['KeySchema']:
            name = data['AttributeName']
            key_type = data['KeyType']
            if key_type == 'HASH':
                hash_key = TableField(name, attrs[name].data_type, key_type)
            elif key_type == 'RANGE':
                range_key = TableField(name, attrs[name].data_type, key_type)
        throughput = description['ProvisionedThroughput']
        return cls(description['IndexName'], description['IndexStatus'],
                   hash_key, range_key, throughput['ReadCapacityUnits'],
                   throughput['WriteCapacityUnits'],
                   description['IndexSizeBytes'], description['ItemCount'])

    def __repr__(self):
        return ("GlobalIndex('%s', '%s', %s, %s, %s, %s)" %
                (self.name, self.status, self.hash_key, self.range_key,
                 self.read_throughput, self.write_throughput))

    def pformat(self):
        """ Pretty format for insertion into table pformat """
        parts = ['GLOBAL INDEX', self.status, self.name]
        keys = "(%s" % self.hash_key.name
        if self.range_key is None:
            keys += ')'
        else:
            keys += ", %s)" % self.range_key.name
        parts.append(keys)
        parts.extend(['THROUGHPUT', "(%d, %d)" % (self.read_throughput,
                                                  self.write_throughput)])

        return ' '.join(parts)

    def __hash__(self):
        return hash(self.name)

    def __eq__(self, other):
        """ Check if schemas are equivalent """
        return (self.name == other.name and
                self.hash_key == other.hash_key and
                self.range_key == other.range_key and
                self.read_throughput == other.read_throughput and
                self.write_throughput == other.write_throughput)

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
        for field in attrs.itervalues():
            if field.key_type == 'HASH':
                self.hash_key = field
            elif field.key_type == 'RANGE':
                self.range_key = field

    @classmethod
    def from_description(cls, description):
        """ Factory method that uses the boto 'describe' return value """
        table = description['Table']
        throughput = table['ProvisionedThroughput']
        attrs = {}
        for data in table['AttributeDefinitions']:
            field = TableField(data['AttributeName'],
                               TYPES[data['AttributeType']])
            attrs[field.name] = field
        for data in table['KeySchema']:
            name = data['AttributeName']
            attrs[name].key_type = data['KeyType']
        for index in table.get('LocalSecondaryIndexes', []):
            for data in index['KeySchema']:
                if data['KeyType'] == 'RANGE':
                    name = data['AttributeName']
                    attrs[name].key_type = 'INDEX'
                    attrs[name].index_name = index['IndexName']
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
        if isinstance(hkey, basestring):
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
        for index in self.global_indexes.itervalues():
            total += index.read_throughput
        return total

    @property
    def total_write_throughput(self):
        """ Combined write throughput of table and global indexes """
        total = self.write_throughput
        for index in self.global_indexes.itervalues():
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
        parts = [
            'CREATE TABLE %s (' % self.name,
        ]
        attrs = ', '.join([attr.schema for attr in self.attrs.itervalues()])
        parts.append(attrs)

        parts.append(", THROUGHPUT (%d, %d))" % (self.read_throughput,
                                                 self.write_throughput))
        gindexes = []
        for gindex in self.global_indexes.itervalues():
            g_parts = []
            g_parts.append("('%s', %s," % (gindex.name, gindex.hash_key.name))
            if gindex.range_key:
                g_parts.append("%s," % gindex.range_key.name)
            g_parts.append("THROUGHPUT (%d, %d))" % (gindex.read_throughput,
                                                     gindex.write_throughput))
            gindexes.append(' '.join(g_parts))
        if gindexes:
            parts.append("GLOBAL INDEX")
            parts.append(', '.join(gindexes))
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

        for gindex in self.global_indexes.itervalues():
            lines.append(gindex.pformat())

        lines.append(str(self.hash_key))
        if self.range_key is not None:
            lines.append(str(self.range_key))

        for field in self.attrs.itervalues():
            if field.key_type == 'INDEX':
                lines.append(str(field))

        for field in self.attrs.itervalues():
            if field.key_type is None:
                lines.append(str(field))

        return '\n'.join(lines)
