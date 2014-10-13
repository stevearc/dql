""" Common use grammars """

from pyparsing import (Word, Upcase, Optional, Combine, Group, alphas, nums,
                       alphanums, quotedString, Keyword, Suppress, Regex,
                       delimitedList, Forward)


def upkey(name):
    """ Shortcut for creating an uppercase keyword """
    return Upcase(Keyword(name, caseless=True))

# pylint: disable=C0103
backtickString = Regex(r'`[^`]*`').setName("string enclosed in backticks")

and_, from_, into, in_, table_key, null, where_ = \
    map(upkey, ['and', 'from', 'into', 'in', 'table', 'null', 'where'])
and_or = and_ | upkey('or')

var = Word(alphas, alphanums + '_-').setName('variable').setResultsName('var')
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
json_primitive = (null.setResultsName('null') |
                  num.setResultsName('number') |
                  quotedString.setResultsName('str') |
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

# Wrap these in a group so they can be used independently
primitive = Group(primitive | expr).setName('primitive')
set_ = Group(set_ | _emptyset | expr).setName('set')
