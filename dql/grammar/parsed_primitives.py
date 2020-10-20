import calendar
import re
from datetime import datetime
from decimal import Decimal

from dateutil.parser import parse
from dateutil.relativedelta import relativedelta
from dateutil.tz import tzlocal, tzutc
from dynamo3 import Binary
from pyparsing import (
    Combine,
    Forward,
    Group,
    Keyword,
    OneOrMore,
    Optional,
    QuotedString,
    Regex,
    Suppress,
    delimitedList,
    oneOf,
    pyparsing_common,
    upcaseTokens,
)

from dql.util import dt_to_ts

from .common import function, quoted, upkey

integer = pyparsing_common.signedInteger
number = integer | pyparsing_common.number.setParseAction(lambda x: Decimal(x[0]))

boolean = (upkey("true") | upkey("false")).setParseAction(lambda x: x == "TRUE")
multiline_string = QuotedString('"', escChar="\\", multiline=True) | QuotedString(
    "'", escChar="\\", multiline=True
)
value = Forward()
json_value = Forward()
string = QuotedString('"', escChar="\\") | QuotedString("'", escChar="\\")
binary = Combine(Suppress("b") + multiline_string).setParseAction(Binary)
null = Keyword("null").setParseAction(lambda _: [None])
json_primitive = number | multiline_string | boolean | null
set_primitive = number | multiline_string | binary
primitive = json_primitive | binary
set_ = (
    (Suppress("(") + Optional(delimitedList(set_primitive)) + Suppress(")"))
    .setName("set")
    .setParseAction(lambda p: set(p.asList()))
)
list_ = (
    Group(Suppress("[") + Optional(delimitedList(json_value)) + Suppress("]"))
    .setName("list")
    .setParseAction(lambda p: p.asList())
)
key_val = Group(QuotedString('"', escChar="\\") + Suppress(":") + json_value)
dict_ = (
    (Suppress("{") + Optional(delimitedList(key_val)) + Suppress("}"))
    .setName("dict")
    .setParseAction(lambda d: {k: v for k, v in d})
)


def make_interval(long_name, short_name):
    """ Create an interval segment """
    pa = lambda x: long_name.upper()
    return (
        integer
        + (
            Keyword(long_name + "s", caseless=True).setParseAction(pa)
            | Regex(long_name + "s", re.I).setParseAction(pa)
            | Keyword(long_name, caseless=True).setParseAction(pa)
            | Regex(long_name, re.I).setParseAction(pa)
            | Keyword(short_name, caseless=True).setParseAction(pa)
            | Regex(short_name, re.I).setParseAction(pa)
        )
    ).setName(long_name)


interval = (
    make_interval("year", "y")
    | make_interval("month", "month")
    | make_interval("week", "w")
    | make_interval("day", "d")
    | make_interval("hour", "h")
    | make_interval("millisecond", "ms")
    | make_interval("minute", "m")
    | make_interval("second", "s")
    | make_interval("microsecond", "us")
)


def eval_interval(result):
    """ Evaluate an interval expression """
    kwargs = {
        "years": 0,
        "months": 0,
        "weeks": 0,
        "days": 0,
        "hours": 0,
        "minutes": 0,
        "seconds": 0,
        "microseconds": 0,
    }
    for i in range(1, len(result), 2):
        amount, key = result[i], result[i + 1]
        if key == "YEAR":
            kwargs["years"] += amount
        elif key == "MONTH":
            kwargs["months"] += amount
        elif key == "WEEK":
            kwargs["weeks"] += amount
        elif key == "DAY":
            kwargs["days"] += amount
        elif key == "HOUR":
            kwargs["hours"] += amount
        elif key == "MINUTE":
            kwargs["minutes"] += amount
        elif key == "SECOND":
            kwargs["seconds"] += amount
        elif key == "MILLISECOND":
            kwargs["microseconds"] += 1000 * amount
        elif key == "MICROSECOND":
            kwargs["microseconds"] += amount
        else:
            raise SyntaxError("Unrecognized interval type %r" % key)
    return relativedelta(**kwargs)  # type: ignore


interval_fxn = (
    (function(upkey("interval"), quoted(OneOrMore(interval)), optparen=True))
    .setName("interval")
    .setParseAction(eval_interval)
)


def eval_expression(result):
    """ Evaluate a full time expression """
    start, op, delta = result
    ts = datetime.fromtimestamp(start).replace(tzinfo=tzlocal())
    if op == "+":
        return dt_to_ts(ts + delta)
    elif op == "-":
        return dt_to_ts(ts - delta)
    else:
        raise SyntaxError("Unrecognized operator %r" % op)


ts_expression = Forward()
ts_functions = (
    function(upkey("timestamp") | upkey("ts"), string, optparen=True).setParseAction(
        lambda x: dt_to_ts(parse(x[1]).replace(tzinfo=tzlocal()))
    )
    | function(
        upkey("utctimestamp") | upkey("utcts"), string, optparen=True
    ).setParseAction(lambda x: dt_to_ts(parse(x[1]).replace(tzinfo=tzutc())))
    | function(upkey("now")).setParseAction(
        lambda _: dt_to_ts(datetime.utcnow().replace(tzinfo=tzutc()))
    )
    | function(upkey("ms"), ts_expression).setParseAction(lambda x: 1000 * x[1])
).setName("function")

ts_expression <<= (ts_functions + oneOf("+ -") + interval_fxn).setParseAction(
    eval_expression
) | ts_functions

json_value <<= json_primitive | list_ | dict_
value <<= (ts_expression | primitive | set_ | list_ | dict_).setName("value")
