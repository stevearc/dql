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
    key_type : str
        The type of key (e.g. 'RANGE', 'HASH', 'INDEX')
    index_name : str, optional
        If the key_type is 'INDEX', this will be the name of the index that
        uses the field as a range key.

    """

    def __init__(self, name, data_type, key_type, index_name=None):
        self.name = name
        self.data_type = data_type
        self.key_type = key_type
        self.index_name = index_name

    def __repr__(self):
        index_name = '' if self.index_name is None else ', ' + self.index_name
        return "TableField(%s, %s, %s%s)" % (self.name, self.data_type,
                                             self.key_type, index_name)

    def __str__(self):
        if self.index_name is not None:
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


class TableMeta(object):

    """
    Container for table metadata

    Parameters
    ----------
    name : str
    size : int
        Size of the table in bytes
    status : str
    hash_key : :class:`.TableField`
    range_key : :class:`.TableField`
    item_count : int
        Number of items in the table
    indexes : dict
        Mapping of attribute name to :class:`.TableField`s
    read_throughput : int
    write_throughput : int
    decreases_today : int

    """

    def __init__(self, name, size, status, hash_key, range_key, item_count,
                 indexes, read_throughput, write_throughput,
                 decreases_today):
        self.name = name
        self.size = size
        self.status = status
        self.hash_key = hash_key
        self.range_key = range_key
        self.item_count = item_count
        self.indexes = indexes
        self.read_throughput = read_throughput
        self.write_throughput = write_throughput
        self.decreases_today = decreases_today

    @classmethod
    def from_description(cls, description):
        """ Factory method that uses the boto 'describe' return value """
        table = description['Table']
        throughput = table['ProvisionedThroughput']
        attrs = {}
        for data in table['AttributeDefinitions']:
            attrs[data['AttributeName']] = TYPES[data['AttributeType']]
        range_key = None
        for data in table['KeySchema']:
            name = data['AttributeName']
            key_type = data['KeyType']
            if key_type == 'HASH':
                hash_key = TableField(name, attrs[name], key_type)
            elif key_type == 'RANGE':
                range_key = TableField(name, attrs[name], key_type)
        indexes = {}
        for index in table.get('LocalSecondaryIndexes', []):
            for data in index['KeySchema']:
                if data['KeyType'] == 'RANGE':
                    name = data['AttributeName']
                    break
            indexes[name] = TableField(name, attrs[name], 'INDEX',
                                       index['IndexName'])
        return cls(table['TableName'], table['TableSizeBytes'],
                   table['TableStatus'], hash_key, range_key,
                   table['ItemCount'], indexes,
                   throughput['ReadCapacityUnits'],
                   throughput['WriteCapacityUnits'],
                   throughput['NumberOfDecreasesToday'])

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

    def __len__(self):
        return self.item_count

    def __repr__(self):
        return 'TableMeta(%s)' % self.name

    def __str__(self):
        return self.name

    def pformat(self):
        """ Pretty string format """
        lines = []
        lines.append(("%s (%s)" % (self.name, self.status)).center(50, '-'))
        lines.append('items: {0:,} ({1:,} bytes)'.format(self.item_count,
                                                         self.size))
        lines.append('read/write: %d/%d' % (self.read_throughput,
                                            self.write_throughput))
        lines.append('decreases remaining: %d' % self.decreases_remaining)
        lines.append(str(self.hash_key))
        if self.range_key is not None:
            lines.append(str(self.range_key))
        lines.extend([str(i) for i in self.indexes.values()])
        return '\n'.join(lines)
