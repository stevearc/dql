""" Grammars for parsing query strings """
from pyparsing import (Group, OneOrMore, delimitedList, Suppress, Optional,
                       oneOf, Forward)

from .common import (var, value, and_, and_or, upkey, set_, not_, types,
                     string, var_val, integer, function)


def create_query_constraint():
    """ Create a constraint for a query WHERE clause """
    op = oneOf('= < > >= <= != <>', caseless=True).setName('operator')
    basic_constraint = (var + op + var_val).setResultsName('operator')
    between = (var + Suppress(upkey('between')) + value + Suppress(and_) +
               value).setResultsName('between')
    is_in = (var + Suppress(upkey('in')) + set_).setResultsName('in')
    fxn = (
        function('attribute_exists', var) |
        function('attribute_not_exists', var) |
        function('attribute_type', var, types) |
        function('begins_with', var, Group(string)) |
        function('contains', var, value) |
        (function('size', var) + op + value)
    ).setResultsName('function')
    all_constraints = (between | basic_constraint | is_in | fxn)
    return Group(all_constraints).setName('constraint')


# pylint: disable=C0103
constraint = create_query_constraint()
# pylint: enable=C0103


def create_where():
    """ Create a grammar for the 'where' clause used by 'select' """
    conjunction = Forward().setResultsName('conjunction')
    nested = Group(Suppress('(') + conjunction + Suppress(')'))\
        .setResultsName('conjunction')

    maybe_nested = (nested | constraint)
    inverted = Group(not_ + maybe_nested).setResultsName('not')
    full_constraint = (maybe_nested | inverted)
    conjunction <<= (full_constraint + OneOrMore(and_or + full_constraint))
    return upkey('where') + Group(conjunction | full_constraint) \
        .setResultsName('where')


def create_keys_in():
    """ Create a grammer for the 'KEYS IN' clause used for queries """
    keys = Group(
        Optional(Suppress('(')) + value + Optional(Suppress(',') + value) +
        Optional(Suppress(')')))
    return (Suppress(upkey('keys') + upkey('in')) + delimitedList(keys))\
        .setResultsName('keys_in')


# pylint: disable=C0103
if_exists = Group(upkey('if') + upkey('exists'))\
    .setResultsName('exists')
if_not_exists = Group(upkey('if') + upkey('not') + upkey('exists'))\
    .setResultsName('not_exists')

where = create_where()
keys_in = create_keys_in()
limit = Group(upkey('limit') + Group(integer)).setResultsName('limit')
