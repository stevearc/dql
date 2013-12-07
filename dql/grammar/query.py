""" Grammars for parsing query strings """
from pyparsing import (Upcase, Group, Forward, ZeroOrMore, delimitedList,
                       Keyword, Suppress, Optional)

from .common import var, op, value, and_, in_


# pylint: disable=C0103
where_ = Upcase(Keyword('where', caseless=True))
# pylint: enable=C0103

# pylint: disable=W0104,W0106


def create_where():
    """ Create the grammar for a 'where' clause """
    where_exp = Forward()
    where_clause = Group(
        (var + op + value) |
        ("(" + where_exp + ")")
    )
    where_exp << where_clause + ZeroOrMore(and_ + where_clause)
    return where_ + where_exp.setResultsName('where')


def create_select_where():
    """ Create a grammar for the 'where' clause used by 'select' """
    clause = Group(var + op + value)
    where_exp = (Optional(Suppress('(')) + clause +
                 ZeroOrMore(Suppress(and_) + clause) + Optional(Suppress(')')))

    # SELECT can also use WHERE KEYS IN ('key1', 'key2'), ('key3', 'key4)
    keys = Group(Suppress('(') + delimitedList(value) + Suppress(')'))
    multiget = (Upcase(Keyword('keys', caseless=True)) + in_ +
                Group(delimitedList(keys)).setResultsName('keys'))

    return where_ + Group(where_exp | multiget).setResultsName('where')


def create_limit():
    """ Create the gramar for a 'limit' clause """
    return Group(Upcase(Keyword('limit', caseless=True)) +
                 value).setResultsName('limit')

# pylint: disable=C0103
using = Upcase(Keyword('using', caseless=True))
if_exists = Group(Upcase(Keyword('if', caseless=True)) +
                  Upcase(Keyword('exists', caseless=True)))\
    .setResultsName('exists')
if_not_exists = Group(Upcase(Keyword('if', caseless=True)) +
                      Upcase(Keyword('not', caseless=True)) +
                      Upcase(Keyword('exists', caseless=True)))\
    .setResultsName('not_exists')

where = create_where()
select_where = create_select_where()
limit = create_limit()
