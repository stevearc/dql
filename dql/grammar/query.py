""" Grammars for parsing query strings """
from pyparsing import (Group, ZeroOrMore, delimitedList, Suppress, Optional,
                       oneOf, Combine)

from .common import var, value, and_, and_or, in_, upkey, where_, primitive, set_


def create_query_constraint():
    """ Create a constraint for a query WHERE clause """
    op = oneOf('= < > >= <=', caseless=True).setName('operator')
    basic_constraint = (var + op + value)
    between = (var + upkey('between') +
               Group(Suppress('(') + value + Suppress(',') + value +
                     Suppress(')')))
    begins_with = (var + Combine(upkey('begins') + upkey('with'), ' ', False) + value)
    return Group(between | basic_constraint | begins_with).setName('constraint')


def create_filter_constraint():
    """ Create a constraint for a scan FILTER clause """
    op = oneOf('= != < > >= <= CONTAINS',
               caseless=True).setName('operator')
    basic_constraint = (var + op + value)
    between = (var + upkey('between') +
               Group(Suppress('(') + value + Suppress(',') + value +
                     Suppress(')')))
    null = (var + upkey('is') + upkey('null'))
    nnull = (var + upkey('is') + Combine(upkey('not') + upkey('null'), ' ', False))
    is_in = (var + upkey('in') + set_)
    ncontains = (var + Combine(upkey('not') + upkey('contains'), ' ', False) + primitive)
    begins_with = (var + Combine(upkey('begins') + upkey('with'), ' ', False) + primitive)
    return Group(between |
                 basic_constraint |
                 begins_with |
                 null |
                 nnull |
                 is_in |
                 ncontains
                 ).setName('constraint')

# pylint: disable=C0103
constraint = create_query_constraint()
filter_constraint = create_filter_constraint()
# pylint: enable=C0103


def create_where():
    """ Create the grammar for a 'where' clause """
    where_exp = Group(constraint +
                      ZeroOrMore(Suppress(and_) + constraint))
    return where_ + where_exp.setResultsName('where')


def create_select_where():
    """ Create a grammar for the 'where' clause used by 'select' """
    where_exp = Group(constraint +
                      ZeroOrMore(Suppress(and_) + constraint))\
        .setResultsName('where')

    # SELECT can also use WHERE KEYS IN ('key1', 'key2'), ('key3', 'key4')
    keys = Group(Suppress('(') + value + Optional(Suppress(',') + value) +
                 Suppress(')'))
    keys_in = (upkey('keys') + in_).setResultsName('keys_in')
    multiget = (keys_in + Group(delimitedList(keys)).setResultsName('where'))

    return where_ + (where_exp | multiget)


def create_filter():
    """ Create a grammar for filtering on table scans """
    filter_exp = Group(Optional(Suppress('(')) + filter_constraint +
                       ZeroOrMore(and_or + filter_constraint) +
                       Optional(Suppress(')')))
    return (upkey('filter') + filter_exp.setResultsName('filter'))


def create_limit():
    """ Create the gramar for a 'limit' clause """
    return Group(upkey('limit') +
                 value).setResultsName('limit')

# pylint: disable=C0103
using = upkey('using')
if_exists = Group(upkey('if') + upkey('exists'))\
    .setResultsName('exists')
if_not_exists = Group(upkey('if') + upkey('not') + upkey('exists'))\
    .setResultsName('not_exists')

where = create_where()
select_where = create_select_where()
filter_ = create_filter()
limit = create_limit()
