""" Common use grammars """
from pyparsing import (Word, upcaseTokens, Optional, Combine, Group, alphas,
                       nums, alphanums, quotedString, Keyword, Suppress, Regex,
                       delimitedList, Forward, oneOf, OneOrMore)


def upkey(name):
    """ Shortcut for creating an uppercase keyword """
    return Keyword(name, caseless=True).setParseAction(upcaseTokens)


def function(name, *args, **kwargs):
    """ Construct a parser for a standard function format """
    if kwargs.get('caseless'):
        name = upkey(name)
    else:
        name = Word(name)
    fxn_args = None
    for i, arg in enumerate(args):
        if i == 0:
            fxn_args = arg
        else:
            fxn_args += Suppress(',') + arg
    if fxn_args is None:
        return name + Suppress('(') + Suppress(')')
    if kwargs.get('optparen'):
        return name + ((Suppress('(') + fxn_args + Suppress(')')) | fxn_args)
    else:
        return name + Suppress('(') + fxn_args + Suppress(')')

# pylint: disable=C0103

and_, or_, from_, into, table_key, null, not_ = \
    map(upkey, ['and', 'or', 'from', 'into', 'table', 'null', 'not'])
and_or = and_ | or_

var = Word(alphas, alphanums + '_-.[]').setName('variable')\
    .setResultsName('var')
table = Word(alphas, alphanums + '_-.').setResultsName('table')
type_ = (upkey('string') |
         upkey('number') |
         upkey('binary'))\
    .setName('type').setResultsName('type')

_sign = Word('+-', exact=1)
number = Combine(Optional(_sign) + Word(nums) +
                 Optional('.' + Optional(Word(nums)))) \
    .setName('number').setResultsName('number')
integer = Combine(Optional(_sign) + Word(nums)) \
    .setName('number').setResultsName('number')
boolean = (upkey('true') | upkey('false')).setName('bool')
binary = Combine('b' + quotedString)

value = Forward()
json_value = Forward()
string = quotedString.setResultsName('str')
json_primitive = (null.setResultsName('null') |
                  number | string |
                  boolean.setResultsName('bool'))
set_primitive = (number.setResultsName('number') |
                 quotedString.setResultsName('str') |
                 binary.setResultsName('binary'))
primitive = (json_primitive | binary.setResultsName('binary'))
_emptyset = Keyword('()').setResultsName('set')
set_ = (Suppress('(') + delimitedList(Group(set_primitive)) +
        Suppress(')')).setResultsName('set')
list_ = (Suppress('[') + Optional(delimitedList(json_value)) +
         Suppress(']')).setResultsName('list')
key_val = (Group(quotedString.setResultsName('str')) + Suppress(':') +
           json_value)
dict_ = (Suppress('{') + Optional(delimitedList(Group(key_val))) +
         Suppress('}')).setResultsName('dict')
json_value <<= Group(json_primitive | list_ | dict_)

ts_functions = Group(
    function('timestamp', quotedString, caseless=True, optparen=True) |
    function('ts', quotedString, caseless=True, optparen=True) |
    function('utctimestamp', quotedString, caseless=True, optparen=True) |
    function('utcts', quotedString, caseless=True, optparen=True) |
    function('now', caseless=True)
).setName('function').setResultsName('ts_function')


def quoted(body):
    """ Quote an item with ' or " """
    return ((Suppress('"') + body + Suppress('"')) |
            (Suppress("'") + body + Suppress("'")))


def make_interval(long_name, short_name):
    """ Create an interval segment """
    return Group(Regex('(-+)?[0-9]+') +
                 (upkey(long_name + 's') |
                  Regex(long_name + 's').setParseAction(upcaseTokens) |
                  upkey(long_name) |
                  Regex(long_name).setParseAction(upcaseTokens) |
                  upkey(short_name) |
                  Regex(short_name).setParseAction(upcaseTokens))) \
        .setResultsName(long_name)
interval = (
    make_interval('year', 'y') |
    make_interval('month', 'month') |
    make_interval('week', 'w') |
    make_interval('day', 'd') |
    make_interval('hour', 'h') |
    make_interval('millisecond', 'ms') |
    make_interval('minute', 'm') |
    make_interval('second', 's') |
    make_interval('microsecond', 'us')
)
intervals = OneOrMore(interval)
interval_fxn = Group(function('interval', quoted(intervals), caseless=True,
                              optparen=True)).setResultsName('interval')
ts_expression = Forward()
ts_expression <<= (Group(ts_functions + oneOf('+ -') + interval_fxn)
                   .setResultsName('ts_expression') |
                   ts_functions |
                   Group(function('ms', Group(ts_expression), caseless=True))
                   .setResultsName('ts_function'))

value <<= Group(ts_expression | primitive | set_ | _emptyset | list_ |
                dict_).setName('value')
var_val = (value | var.setResultsName('field'))

# Wrap these in a group so they can be used independently
primitive = Group(primitive).setName('primitive')
set_ = Group(set_ | _emptyset).setName('set')
types = oneOf('s ss n ns b bs bool null l m',
              caseless=True).setParseAction(upcaseTokens)
filename = (quotedString | Regex(r'[0-9A-Za-z/_\-\.]+'))
