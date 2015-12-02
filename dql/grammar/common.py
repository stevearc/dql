""" Common use grammars """

from pyparsing import (Word, Upcase, Optional, Combine, Group, alphas, nums,
                       alphanums, quotedString, Keyword, Suppress, Regex,
                       delimitedList, Forward, oneOf)


def upkey(name):
    """ Shortcut for creating an uppercase keyword """
    return Upcase(Keyword(name, caseless=True))

# pylint: disable=C0103
backtickString = Regex(r'`[^`]*`').setName("string enclosed in backticks")

and_, or_, from_, into, in_, table_key, null, not_ = \
    map(upkey, ['and', 'or', 'from', 'into', 'in', 'table', 'null', 'not'])
and_or = and_ | or_

var = Word(alphas, alphanums + '_-.[]').setName('variable')\
    .setResultsName('var')
expr = Combine(
    Optional('m') +
    backtickString).setName('python expression').setResultsName('python')
table = Word(alphas, alphanums + '_-.').setResultsName('table')
type_ = (upkey('string') |
         upkey('number') |
         upkey('binary'))\
    .setName('type').setResultsName('type')

_sign = Word('+-', exact=1)
num = Combine(Optional(_sign) + Word(nums) +
              Optional('.' + Optional(Word(nums)))).setName('number')
boolean = (upkey('true') | upkey('false')).setName('bool')
binary = Combine('b' + quotedString)

value = Forward()
json_value = Forward()
string = quotedString.setResultsName('str')
json_primitive = (null.setResultsName('null') |
                  num.setResultsName('number') |
                  string |
                  boolean.setResultsName('bool'))
set_primitive = (num.setResultsName('number') |
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
value <<= Group(primitive | expr | set_ | _emptyset | list_ |
                dict_).setName('value')
var_val = (var.setResultsName('field') | value)

# Wrap these in a group so they can be used independently
primitive = Group(primitive | expr).setName('primitive')
set_ = Group(set_ | _emptyset | expr).setName('set')
types = Upcase(oneOf('s ss n ns b bs bool null l m', caseless=True))
