""" Grammars for parsing query strings """
from pyparsing import (CaselessLiteral, Upcase, Group, Forward, ZeroOrMore,
                       Keyword)

from .common import var, op, value, and_


# pylint: disable=W0104,W0106

def create_where():
    """ Create the grammar for a 'where' clause """
    where_exp = Forward()
    where_clause = Group(
        (var + op + value) |
        ("(" + where_exp + ")")
    )
    where_exp << where_clause + ZeroOrMore(and_ + where_clause)
    return Upcase(Keyword('where', caseless=True)) + where_exp.setResultsName('where')


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
limit = create_limit()
