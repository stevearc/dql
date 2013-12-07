""" DQL language parser """
from pyparsing import (Upcase, delimitedList, Optional, Group, Forward,
                       restOfLine, Keyword, LineEnd, Suppress, ZeroOrMore)

from .common import (and_, op, from_, table, var, value, table_key, into,
                     type_)
from .query import where, limit, if_exists, if_not_exists, using


# pylint: disable=W0104,W0106

def create_select():
    """ Create the grammar for the 'select' statement """
    select = Upcase(Keyword("select", caseless=True)).setResultsName('action')
    attrs = Group(Keyword('*') |
                  (Optional(Suppress('(')) + delimitedList(var) +
                   Optional(Suppress(')'))))\
        .setResultsName('attrs')

    return (select + attrs + from_ + table + where +
            Optional(using + value).setResultsName('using') +
            Optional(limit))


def create_scan():
    """ Create the grammar for the 'scan' statement """
    scan = Upcase(Keyword("scan", caseless=True)).setResultsName('action')
    filter_exp = Forward()
    filter_clause = Group(
        (var + op + value) |
        ("(" + filter_exp + ")")
    )
    filter_exp << filter_clause + ZeroOrMore(and_ + filter_clause)
    filter_ = (Upcase(Keyword('filter', caseless=True)) +
               filter_exp.setResultsName('filter'))

    return (scan + table + Optional(filter_) + Optional(limit))


def create_count():
    """ Create the grammar for the 'count' statement """
    count = Upcase(Keyword("count", caseless=True)).setResultsName('action')

    return (count + table + where +
            Optional(using + value).setResultsName('using'))


def create_create():
    """ Create the grammar for the 'create' statement """
    create = Upcase(Keyword("create", caseless=True)).setResultsName('action')
    hash_key = Group(Upcase(Keyword("hash", caseless=True)) +
                     Upcase(Keyword('key', caseless=True)))
    range_key = Group(Upcase(Keyword("range", caseless=True)) +
                      Upcase(Keyword('key', caseless=True)))

    # ATTR DECLARATION
    index = Group(Upcase(Keyword('index', caseless=True)) + Suppress('(') +
                  value + Suppress(')'))
    index_type = (hash_key | range_key | index)\
        .setName('index specification').setResultsName('index')
    attr_declaration = Group(var.setResultsName('name') + type_ + index_type)\
        .setName('attr').setResultsName('attr')
    attrs_declaration = Group(delimitedList(attr_declaration))\
        .setName('attrs').setResultsName('attrs')

    return (create + table_key + Optional(if_not_exists) + table +
            '(' + attrs_declaration + ')')


def create_delete():
    """ Create the grammar for the 'delete' statement """
    delete = Upcase(Keyword("delete", caseless=True)).setResultsName('action')
    return (delete + from_ + table + where +
            Optional(using + value).setResultsName('using'))


def create_insert():
    """ Create the grammar for the 'insert' statement """
    insert = Upcase(Keyword("insert", caseless=True)).setResultsName('action')
    attrs = Group(delimitedList(var)).setResultsName('attrs')

    # VALUES
    values_key = Upcase(Keyword("values", caseless=True))
    value_group = Group(Suppress('(') + delimitedList(value) + Suppress(')'))
    values = Group(delimitedList(value_group)).setResultsName('data')

    return (insert + into + table + Suppress('(') + attrs + Suppress(')') +
            values_key + values)


def create_drop():
    """ Create the grammar for the 'drop' statement """
    drop = Upcase(Keyword("drop", caseless=True)).setResultsName('action')
    return (drop + table_key + Optional(if_exists) + table)


def create_update():
    """ Create the grammar for the 'update' statement """
    update = Upcase(Keyword("update", caseless=True)).setResultsName('action')
    return (update + table + where)


def create_parser():
    """ Create the language parser """
    dql = ((create_select() |
            create_scan() |
            create_count() |
            create_delete() |
            create_update() |
            create_create() |
            create_insert() |
            create_drop()) +
           Suppress(LineEnd()))

    dql.ignore('--' + restOfLine)

    return dql

parser = create_parser()  # pylint: disable=C0103
