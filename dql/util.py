""" Utility methods """
import six
from decimal import Decimal
from dynamo3 import Binary


def resolve(val):
    """ Convert a pyparsing value to the python type """
    name = val.getName()
    if name == 'number':
        try:
            return int(val.number)
        except ValueError:
            return Decimal(val.number)
    elif name == 'str':
        return val.str[1:-1]
    elif name == 'null':
        return None
    elif name == 'binary':
        return Binary(val.binary[2:-1])
    elif name == 'set':
        if val.set == '()':
            return set()
        return set([resolve(v) for v in val.set])
    elif name == 'bool':
        return val.bool == 'TRUE'
    elif name == 'list':
        return [resolve(v) for v in val.list]
    elif name == 'dict':
        dict_val = {}
        for k, v in val.dict:
            dict_val[resolve(k)] = resolve(v)
        return dict_val
    else:
        raise SyntaxError("Unable to resolve value '%s'" % val)


def plural(value, append='s'):
    """ Helper function for pluralizing text """
    return '' if value == 1 else append


def pretty_format(statement, result):
    """ Format the return value of a query for humans """
    if result is None:
        return 'Success'
    if statement.action == 'UPDATE':
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
