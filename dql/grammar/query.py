""" Grammars for parsing query strings """
from pyparsing import Group, ZeroOrMore, delimitedList, Suppress, Optional

from .common import var, op, value, and_, in_, upkey, where_


# pylint: disable=W0104,W0106

def create_where():
    """ Create the grammar for a 'where' clause """
    clause = Group(var + op + value)
    where_exp = Group(Optional(Suppress('(')) + clause +
                      ZeroOrMore(Suppress(and_) + clause) +
                      Optional(Suppress(')')))
    return where_ + where_exp.setResultsName('where')


def create_select_where():
    """ Create a grammar for the 'where' clause used by 'select' """
    clause = Group(var + op + value)
    where_exp = Group(Optional(Suppress('(')) + clause +
                      ZeroOrMore(Suppress(and_) + clause) +
                      Optional(Suppress(')')))\
        .setResultsName('where')

    # SELECT can also use WHERE KEYS IN ('key1', 'key2'), ('key3', 'key4)
    keys = Group(Suppress('(') + delimitedList(value) + Suppress(')'))
    keys_in = (upkey('keys') + in_).setResultsName('keys_in')
    multiget = (keys_in + Group(delimitedList(keys)).setResultsName('where'))

    return where_ + (where_exp | multiget)


def create_filter():
    """ Create a grammar for filtering on table scans """
    clause = Group(var + op + value)
    filter_exp = Group(Optional(Suppress('(')) + clause +
                       ZeroOrMore(Suppress(and_) + clause) +
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
