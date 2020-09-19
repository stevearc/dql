# -*- coding: utf-8 -*-
""" Formatting and displaying output """
import contextlib
import json
import locale
import os
import stat
import subprocess
import sys
import tempfile
from base64 import b64encode
from builtins import input, range, str
from collections import OrderedDict
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Dict

from dateutil.relativedelta import relativedelta
from dynamo3 import Binary
from rich.console import Console

from .util import getmaxyx, plural

console = Console()


def truncate(string, length, ellipsis="…"):
    """ Truncate a string to a length, ending with '...' if it overflows """
    if len(string) > length:
        return string[: length - len(ellipsis)] + ellipsis
    return string


def make_list(obj):
    """ Turn an object into a list if it isn't already """
    if isinstance(obj, list):
        return obj
    else:
        return list(obj)


def wrap(string, length, indent):
    """ Wrap a string at a line length """
    newline = "\n" + " " * indent
    return newline.join((string[i : i + length] for i in range(0, len(string), length)))


def serialize_json_var_lossy_float(obj):
    """ Serialize custom types to JSON """
    if isinstance(obj, Decimal):
        return float(obj)
    elif isinstance(obj, bytes):
        return b64encode(obj).decode("ascii")
    elif isinstance(obj, Binary):
        return b64encode(obj.value).decode("ascii")
    elif isinstance(obj, set):
        return list(obj)
    else:
        raise TypeError("%s %r is not JSON serializable" % (type(obj), obj))


def serialize_json_var(obj):
    """ Serialize custom types to JSON """
    if isinstance(obj, Decimal):
        return str(obj)
    return serialize_json_var_lossy_float(obj)


def format_json(json_object, indent, default):
    """ Pretty-format json data """
    indent_str = "\n" + " " * indent
    json_str = json.dumps(json_object, indent=2, default=default)
    return indent_str.join(json_str.split("\n"))


def delta_to_str(rd):
    """ Convert a relativedelta to a human-readable string """
    parts = []
    if rd.days > 0:
        parts.append("%d day%s" % (rd.days, plural(rd.days)))
    clock_parts = []
    if rd.hours > 0:
        clock_parts.append("%02d" % rd.hours)
    if rd.minutes > 0 or rd.hours > 0:
        clock_parts.append("%02d" % rd.minutes)
    if rd.seconds > 0 or rd.minutes > 0 or rd.hours > 0:
        clock_parts.append("%02d" % rd.seconds)
    if clock_parts:
        parts.append(":".join(clock_parts))
    return " ".join(parts)


class BaseFormat(object):

    """ Base class for formatters """

    def __init__(
        self, results, ostream, width="auto", pagesize="auto", lossy_json_float=True
    ):
        self._results = make_list(results)
        self._ostream = ostream
        self._width = width
        self._pagesize = pagesize
        self._lossy_json_float = lossy_json_float

    @property
    def _default_json_serializer(self):
        """ Getter for _default_json_serializer """
        if self._lossy_json_float:
            return serialize_json_var_lossy_float
        else:
            return serialize_json_var

    @property
    def width(self):
        """ The display width """
        if self._width == "auto":
            return getmaxyx()[1]
        return self._width

    @property
    def pagesize(self):
        """ The number of results to display at a time """
        if self._pagesize == "auto":
            return getmaxyx()[0] - 5
        return self._pagesize

    def pre_write(self):
        """ Called once before writing the very first record """

    def post_write(self):
        """ Called once after writing all records """

    def display(self):
        """ Write results to an output stream """
        total = 0
        count = 0
        for i, result in enumerate(self._results):
            if total == 0:
                self.pre_write()
            self.write(result)
            count += 1
            total += 1
            if (
                count >= self.pagesize
                and self.pagesize > 0
                and i < len(self._results) - 1
            ):
                self.wait()
                count = 0
        if total == 0:
            self._ostream.write("No results\n")
        else:
            self.post_write()

    def wait(self):
        """ Block for user input """
        text = input(
            "Press return for next %d result%s (or type 'all'):"
            % (self.pagesize, plural(self.pagesize))
        )
        if text:
            if text.lower() in ["a", "all"]:
                self._pagesize = 0
            elif text.isdigit():
                self._pagesize = int(text)

    def write(self, result):
        """ Write a single result and stick it in an output stream """
        raise NotImplementedError

    def format_field(self, field):
        """ Format a single Dynamo value """
        if field is None:
            return "NULL"
        elif isinstance(field, TypeError):
            return "TypeError"
        elif isinstance(field, Decimal):
            if field % 1 == 0:
                return str(int(field))
            return str(float(field))
        elif isinstance(field, set):
            return "(" + ", ".join([self.format_field(v) for v in field]) + ")"
        elif isinstance(field, datetime):
            return field.isoformat()
        elif isinstance(field, timedelta):
            rd = relativedelta(
                seconds=int(field.total_seconds()), microseconds=field.microseconds
            )
            return delta_to_str(rd)
        elif isinstance(field, Binary):
            return "<Binary %d>" % len(field.value)
        pretty = repr(field)
        if pretty.startswith("u'"):
            return pretty[1:]
        return pretty


class ExpandedFormat(BaseFormat):

    """ A layout that puts item attributes on separate lines """

    @property
    def pagesize(self):
        if self._pagesize == "auto":
            return 1
        return self._pagesize

    def write(self, result):
        self._ostream.write(self.width * "-" + "\n")
        max_key = max((len(k) for k in result.keys()))
        for key, val in result.items():
            # If the value is json, try to unpack it and format it better.
            if isinstance(val, str) and val.startswith("{"):
                try:
                    data = json.loads(val)
                except ValueError:
                    pass
                else:
                    val = format_json(
                        data, max_key + 3, default=self._default_json_serializer
                    )
            elif isinstance(val, (dict, list)):
                val = format_json(
                    val, max_key + 3, default=self._default_json_serializer
                )
            else:
                val = wrap(
                    self.format_field(val), self.width - max_key - 3, max_key + 3
                )
            self._ostream.write("{0} : {1}\n".format(key.rjust(max_key), val))


class ColumnFormat(BaseFormat):

    """ A layout that puts item attributes in columns """

    def __init__(self, *args, **kwargs):
        super(ColumnFormat, self).__init__(*args, **kwargs)
        col_width: Dict[str, int] = OrderedDict()
        for result in self._results:
            for key, value in result.items():
                col_width.setdefault(key, len(key))
                col_width[key] = max(col_width[key], len(self.format_field(value)))
        self._all_columns = list(col_width)
        self.width_requested = 3 + len(col_width) + sum(col_width.values())
        if self.width_requested > self.width:
            even_width = int((self.width - 1) / len(self._all_columns)) - 3
            for key in col_width:
                col_width[key] = even_width
        self._col_width = col_width

        header = "|"
        for col in self._all_columns:
            width = self._col_width[col]
            header += " "
            header += truncate(col.center(width), width)
            header += " |"
        self._header = header

    def _write_header(self):
        """ Write out the table header """
        self._ostream.write(len(self._header) * "-" + "\n")
        self._ostream.write(self._header)
        self._ostream.write("\n")
        self._ostream.write(len(self._header) * "-" + "\n")

    def _write_footer(self):
        """ Write out the table footer """
        self._ostream.write(len(self._header) * "-" + "\n")

    def pre_write(self):
        self._write_header()

    def post_write(self):
        self._write_footer()

    def wait(self):
        """ Block for user input """
        self._write_footer()
        super(ColumnFormat, self).wait()
        self._write_header()

    def write(self, result):
        self._ostream.write("|")
        for col, width in self._col_width.items():
            self._ostream.write(" ")
            val = self.format_field(result.get(col, None)).ljust(width)
            self._ostream.write(truncate(val, width))
            self._ostream.write(" |")
        self._ostream.write("\n")


class JsonFormat(BaseFormat):
    def write(self, result):
        pass

    def display(self):
        for result in self._results:
            self._ostream.write(
                json.dumps(result, default=self._default_json_serializer)
            )
            self._ostream.write("\n")


class SmartFormat(object):

    """ A layout that chooses column/expanded format intelligently """

    _sub_formatter: BaseFormat

    def __init__(self, results, ostream, *args, **kwargs):
        results = make_list(results)
        fmt = ColumnFormat(results, ostream, *args, **kwargs)
        if fmt.width_requested > fmt.width:
            self._sub_formatter = ExpandedFormat(results, ostream, *args, **kwargs)
        else:
            self._sub_formatter = fmt

    def display(self):
        """ Write results to an output stream """
        self._sub_formatter.display()


class SmartBuffer(object):

    """ A buffer that wraps another buffer and encodes unicode strings. """

    def __init__(self, buf):
        self._buffer = buf
        try:
            self.encoding = locale.getdefaultlocale()[1] or "utf-8"
        except ValueError:
            self.encoding = "utf-8"

    def write(self, arg):
        """ Write a string or bytes object to the buffer """
        if isinstance(arg, str):
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
    outfile = os.fdopen(os.open(filename, os.O_WRONLY | os.O_CREAT, mode), "wb")
    try:
        yield SmartBuffer(outfile)
        outfile.flush()
        subprocess.call(["less", "-FXR", filename])
    finally:
        if outfile is not None:
            outfile.close()
        if os.path.exists(filename):
            os.unlink(filename)


@contextlib.contextmanager
def stdout_display():
    """ Print results straight to stdout """
    yield SmartBuffer(sys.stdout.buffer)
