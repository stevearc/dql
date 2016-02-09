""" Wrapper around the dynamo3 RateLimit class """

import six
from dynamo3 import RateLimit


@six.python_2_unicode_compatible
class TableLimits(object):
    """ Wrapper around :class:`dynamo3.RateLimit` """

    def __init__(self):
        self.total = {}
        self.default = {}
        self.indexes = {}
        self.tables = {}

    def _compute_limit(self, limit, throughput):
        """ Compute a percentage limit or return a point limit """
        if limit[-1] == '%':
            return throughput * float(limit[:-1]) / 100.
        else:
            return float(limit)

    def get_limiter(self, table_descriptions):
        """ Construct a RateLimit object from the throttle declarations """
        table_caps = {}
        for table in table_descriptions:
            limit = self.tables.get(table.name) or self.default
            # Add the table limit
            if limit:
                table_caps[table.name] = {
                    'read': self._compute_limit(limit['read'],
                                                table.read_throughput),
                    'write': self._compute_limit(limit['write'],
                                                 table.write_throughput),
                }
            if table.name not in self.indexes:
                continue
            # Add the global index limits
            for index in six.itervalues(table.global_indexes):
                limit = (self.indexes[table.name].get(index.name) or
                         self.default)
                if limit:
                    cap = table_caps.setdefault(table.name, {})
                    cap[index.name] = {
                        'read': self._compute_limit(limit['read'],
                                                    index.read_throughput),
                        'write': self._compute_limit(limit['write'],
                                                     index.write_throughput),
                    }
        kwargs = {
            'table_caps': table_caps,
        }
        if self.total:
            kwargs['total_read'] = float(self.total['read'])
            kwargs['total_write'] = float(self.total['write'])
        return RateLimit(**kwargs)

    def __nonzero__(self):
        return (bool(self.tables) or bool(self.indexes) or
                bool(self.default) or bool(self.total))

    def _set_limit(self, data, key, read, write):
        """ Set a limit or delete if non provided """
        if read != '0' or write != '0':
            data[key] = {
                'read': read,
                'write': write,
            }
        elif key in data:
            del data[key]

    def set_default_limit(self, read='0', write='0'):
        """ Set the default table/index limit """
        if read == '0' and write == '0':
            self.default = {}
            return
        self.default = {
            'read': read,
            'write': write,
        }

    def set_total_limit(self, read='0', write='0'):
        """ Set the total throughput limit """
        if read == '0' and write == '0':
            self.total = {}
            return
        if not read.isdigit() or not write.isdigit():
            raise ValueError("Total read/write limits must be a point value")
        self.total = {
            'read': read,
            'write': write,
        }

    def set_table_limit(self, tablename, read='0', write='0'):
        """ Set the limit on a table """
        self._set_limit(self.tables, tablename, read, write)

    def set_index_limit(self, tablename, indexname, read='0', write='0'):
        """ Set the limit on a global index """
        index_data = self.indexes.setdefault(tablename, {})
        self._set_limit(index_data, indexname, read, write)
        if not index_data:
            del self.indexes[tablename]

    def load(self, data):
        """ Load the configuration from a save() dict """
        self.total = data.get('total', {})
        self.default = data.get('default', {})
        self.tables = {}
        self.indexes = {}
        for tablename, limit in six.iteritems(data.get('tables', {})):
            self.set_table_limit(tablename, **limit)
        for tablename, index_data in six.iteritems(data.get('indexes', {})):
            for indexname, limit in six.iteritems(index_data):
                self.set_index_limit(tablename, indexname, **limit)

    def __str__(self):
        lines = []
        if self.total:
            lines.append("Total: %(read)s, %(write)s" % self.total)
        if self.default:
            lines.append("Default: %(read)s, %(write)s" % self.default)
        for tablename, limit in six.iteritems(self.tables):
            lines.append("%s: %s, %s" % (tablename, limit['read'],
                                         limit['write']))
            indexes = self.indexes.get(tablename, {})
            for indexname, limit in six.iteritems(indexes):
                lines.append("%s:%s: %s, %s" % (tablename, indexname,
                                                limit['read'], limit['write']))

        # Add all the throttled indexes that don't have their table throttled.
        for tablename, data in six.iteritems(self.indexes):
            if tablename in self.tables:
                continue
            for indexname, limit in six.iteritems(data):
                lines.append("%s:%s: %s, %s" % (tablename, indexname,
                                                limit['read'], limit['write']))
        if lines:
            return '\n'.join(lines)
        else:
            return "No throttle"

    def save(self):
        """ Wrapper around __json__ """
        return self.__json__()

    def __json__(self, *_):
        """ I dunno, I guess I thought this was useful. """
        return {
            'tables': self.tables,
            'indexes': self.indexes,
            'total': self.total,
            'default': self.default,
        }
