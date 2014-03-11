# -*- coding: utf-8 -*-
""" Formatting and displaying output """
from __future__ import unicode_literals

import contextlib
import io
import subprocess
import sys
import tempfile
from decimal import Decimal
from distutils.spawn import find_executable  # pylint: disable=E0611,F0401

import six

import locale
import os
import stat


def truncate(string, length, ellipsis='…'):
    """ Truncate a string to a length, ending with '...' if it overflows """
    if len(string) > length:
        return string[:length - len(ellipsis)] + ellipsis
    return string


def wrap(string, length, indent):
    """ Wrap a string at a line length """
    newline = '\n' + ' ' * indent
    return newline.join((string[i:i + length]
                         for i in xrange(0, len(string), length)))


class BaseFormat(object):

    """ Base class for formatters """

    def __init__(self, width=100, pagesize=1000):
        self.width = width
        self.pagesize = pagesize

    def write(self, results, ostream):
        """ Write results to an output stream """

        count = 0
        for result in results:
            self.format(result, ostream)
            count += 1
            if count > self.pagesize:
                return True
        return False

    def format(self, result, ostream):
        """ Format a single result and stick it in an output stream """
        raise NotImplementedError

    def format_field(self, field):
        """ Format a single Dynamo value """
        if isinstance(field, Decimal):
            if field % 1 == 0:
                return unicode(int(field))
            return unicode(float(field))
        pretty = repr(field)
        if pretty.startswith("u'"):
            return pretty[1:]
        return pretty


class ExpandedFormat(BaseFormat):

    """ A layout that puts item attributes on separate lines """

    def format(self, result, ostream):
        ostream.write(self.width * '-' + '\n')
        max_key = max((len(k) for k in result.keys()))
        for key, val in sorted(result.items()):
            val = wrap(self.format_field(val), self.width - max_key - 3,
                       max_key + 3)
            ostream.write("{0} : {1}\n".format(key.rjust(max_key), val))


class ColumnFormat(BaseFormat):

    """ A layout that puts item attributes in columns """

    def write(self, results, ostream):
        count = 0
        to_format = []
        all_columns = set()
        retval = False
        for result in results:
            to_format.append(result)
            all_columns.update(result.keys())
            count += 1
            if count > self.pagesize:
                retval = True
                break
        if to_format:
            self.format(to_format, all_columns, ostream)
        return retval

    def format(self, results, columns, ostream):
        col_width = int((self.width - 1) / len(columns)) - 3

        # Print the header
        header = '|'
        for col in columns:
            header += ' '
            header += truncate(col.center(col_width), col_width)
            header += ' |'
        ostream.write(len(header) * '-' + '\n')
        ostream.write(header)
        ostream.write('\n')
        ostream.write(len(header) * '-' + '\n')

        for result in results:
            ostream.write('|')
            for col in columns:
                ostream.write(' ')
                val = self.format_field(result.get(
                    col, None)).ljust(col_width)
                ostream.write(truncate(val, col_width))
                ostream.write(' |')
            ostream.write('\n')
        ostream.write(len(header) * '-' + '\n')


class SmartFormat(ColumnFormat):

    """ A layout that chooses column/expanded format intelligently """

    def __init__(self, *args, **kwargs):
        super(SmartFormat, self).__init__(*args, **kwargs)

    def format(self, results, columns, ostream):
        col_width = int((self.width - 2) / len(columns))
        if col_width < 10:
            expanded = ExpandedFormat(self.width, self.pagesize)
            for result in results:
                expanded.format(result, ostream)
        else:
            super(SmartFormat, self).format(results, columns, ostream)


def get_default_display():
    """ Get the default display function for this system """
    if find_executable('less'):
        return less_display
    else:
        return stdout_display


class SmartBuffer(object):

    """ A buffer that wraps another buffer and encodes unicode strings. """

    def __init__(self, buf):
        self._buffer = buf
        self.encoding = locale.getdefaultlocale()[1] or 'utf-8'

    def write(self, arg):
        """ Write a string or bytes object to the buffer """
        if isinstance(arg, six.text_type):
            arg = arg.encode(self.encoding)
        return self._buffer.write(arg)

    def flush(self):
        """ flush the buffer """
        return self._buffer.flush()


@contextlib.contextmanager
def less_display():
    """ Use smoke and mirrors to acquire 'less' for pretty paging """
    # here's some magic. We want the nice paging from 'less', so we write
    # the output to a file and use subprocess to run 'less' on the file.
    # But the file might have sensitive data, so open it in 0600 mode.
    _, filename = tempfile.mkstemp()
    mode = stat.S_IRUSR | stat.S_IWUSR
    outfile = None
    outfile = os.fdopen(os.open(filename,
                                os.O_WRONLY | os.O_CREAT, mode), 'wb')
    try:
        yield SmartBuffer(outfile)
        outfile.flush()
        subprocess.call(['less', '-FXR', filename])
    finally:
        if outfile is not None:
            outfile.close()
        if os.path.exists(filename):
            os.unlink(filename)


@contextlib.contextmanager
def stdout_display():
    """ Print results straight to stdout """
    yield SmartBuffer(sys.stdout)
