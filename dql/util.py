""" Utility methods """
import calendar
from datetime import datetime
from dateutil.parser import parse
from dateutil.relativedelta import relativedelta
from dateutil.tz import tzlocal, tzutc
from decimal import Decimal
from dynamo3 import Binary


try:
    from shutil import get_terminal_size  # pylint: disable=E0611

    def getmaxyx():
        """ Get the terminal height and width """
        size = get_terminal_size()
        return size[1], size[0]
except ImportError:
    try:
        import os
        from fcntl import ioctl
        from termios import TIOCGWINSZ
        import struct

        def getmaxyx():
            """ Get the terminal height and width """
            try:
                return int(os.environ["LINES"]), int(os.environ["COLUMNS"])
            except KeyError:
                height, width = \
                    struct.unpack("hhhh", ioctl(0, TIOCGWINSZ, 8 * "\000"))[0:2]
                if not height or not width:
                    return 25, 80
                return height, width
    except ImportError:
        # Windows doesn't have fcntl or termios, so fall back to defaults.
        def getmaxyx():
            """ Get the terminal height and width """
            return 25, 80


def plural(value, append='s'):
    """ Helper function for pluralizing text """
    return '' if value == 1 else append


def unwrap(value):
    """ Unwrap a quoted string """
    return value[1:-1]


def resolve(val):
    """ Convert a pyparsing value to the python type """
    name = val.getName()
    if name == 'number':
        try:
            return int(val.number)
        except ValueError:
            return Decimal(val.number)
    elif name == 'str':
        return unwrap(val.str)
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
    elif name == 'ts_function':
        return dt_to_ts(eval_function(val.ts_function))
    elif name == 'ts_expression':
        return dt_to_ts(eval_expression(val))
    else:
        raise SyntaxError("Unable to resolve value '%s'" % val)


def dt_to_ts(value):
    """ If value is a datetime, convert to timestamp """
    if not isinstance(value, datetime):
        return value
    return calendar.timegm(value.utctimetuple()) + value.microsecond / 1000000.


def eval_function(value):
    """ Evaluate a timestamp function """
    name, args = value[0], value[1:]
    if name == 'NOW':
        return datetime.utcnow().replace(tzinfo=tzutc())
    elif name in ['TIMESTAMP', 'TS']:
        return parse(unwrap(args[0])).replace(tzinfo=tzlocal())
    elif name in ['UTCTIMESTAMP', 'UTCTS']:
        return parse(unwrap(args[0])).replace(tzinfo=tzutc())
    elif name == 'MS':
        return 1000 * resolve(args[0])
    else:
        raise SyntaxError("Unrecognized function %r" % name)


def eval_interval(interval):
    """ Evaluate an interval expression """
    kwargs = {
        'years': 0,
        'months': 0,
        'weeks': 0,
        'days': 0,
        'hours': 0,
        'minutes': 0,
        'seconds': 0,
        'microseconds': 0,
    }
    for section in interval[1:]:
        name = section.getName()
        if name == 'year':
            kwargs['years'] += int(section[0])
        elif name == 'month':
            kwargs['months'] += int(section[0])
        elif name == 'week':
            kwargs['weeks'] += int(section[0])
        elif name == 'day':
            kwargs['days'] += int(section[0])
        elif name == 'hour':
            kwargs['hours'] += int(section[0])
        elif name == 'minute':
            kwargs['minutes'] += int(section[0])
        elif name == 'second':
            kwargs['seconds'] += int(section[0])
        elif name == 'millisecond':
            kwargs['microseconds'] += 1000 * int(section[0])
        elif name == 'microsecond':
            kwargs['microseconds'] += int(section[0])
        else:
            raise SyntaxError("Unrecognized interval type %r: %s" %
                              (name, section))
    return relativedelta(**kwargs)


def eval_expression(value):
    """ Evaluate a full time expression """
    start = eval_function(value.ts_expression[0])
    interval = eval_interval(value.ts_expression[2])
    op = value.ts_expression[1]
    if op == '+':
        return start + interval
    elif op == '-':
        return start - interval
    else:
        raise SyntaxError("Unrecognized operator %r" % op)
