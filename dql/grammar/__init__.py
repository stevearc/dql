""" DQL language parser """
from pyparsing import (delimitedList, Optional, Group, Forward, restOfLine,
                       Keyword, LineEnd, Suppress, ZeroOrMore, oneOf)

from .common import (and_, op, from_, table, var, value, table_key, into,
                     type_, upkey)
from .query import (where, select_where, limit, if_exists, if_not_exists,
                    using, filter_)


# pylint: disable=W0104,W0106

def create_select():
    """ Create the grammar for the 'select' statement """
    select = upkey('select').setResultsName('action')
    attrs = Group(Keyword('*') |
                  (Optional(Suppress('(')) + delimitedList(var) +
                   Optional(Suppress(')'))))\
        .setResultsName('attrs')

    return (select + attrs + from_ + table + select_where +
            Optional(using + value).setResultsName('using') +
            Optional(limit))


def create_scan():
    """ Create the grammar for the 'scan' statement """
    scan = upkey('scan').setResultsName('action')
    return (scan + table + Optional(filter_) + Optional(limit))


def create_count():
    """ Create the grammar for the 'count' statement """
    count = upkey('count').setResultsName('action')

    return (count + table + where +
            Optional(using + value).setResultsName('using'))


def create_create():
    """ Create the grammar for the 'create' statement """
    create = upkey('create').setResultsName('action')
    hash_key = Group(upkey('hash') +
                     upkey('key'))
    range_key = Group(upkey('range') +
                      upkey('key'))

    # ATTR DECLARATION
    index = Group(upkey('index') + Suppress('(') +
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
    delete = upkey('delete').setResultsName('action')
    return (delete + from_ + table + select_where +
            Optional(using + value).setResultsName('using'))


def create_insert():
    """ Create the grammar for the 'insert' statement """
    insert = upkey('insert').setResultsName('action')
    attrs = Group(delimitedList(var)).setResultsName('attrs')

    # VALUES
    values_key = upkey('values')
    value_group = Group(Suppress('(') + delimitedList(value) + Suppress(')'))
    values = Group(delimitedList(value_group)).setResultsName('data')

    return (insert + into + table + Suppress('(') + attrs + Suppress(')') +
            values_key + values)


def create_drop():
    """ Create the grammar for the 'drop' statement """
    drop = upkey('drop').setResultsName('action')
    return (drop + table_key + Optional(if_exists) + table)


def create_update():
    """ Create the grammar for the 'update' statement """
    update = upkey('update').setResultsName('action')
    returns, none, set_, all_, updated, old, new = \
        map(upkey, ['returns', 'none', 'set', 'all', 'updated', 'old',
                    'new'])
    set_op = oneOf('= += -=', caseless=True).setName('operator')
    clause = Group(var + set_op + value)
    set_values = Group(delimitedList(clause)).setResultsName('updates')
    return_ = returns + Group(none |
                             (all_ + old) |
                             (all_ + new) |
                             (updated + old) |
                             (updated + new))\
        .setResultsName('returns')
    return (update + table + set_ + set_values + Optional(select_where) +
            Optional(return_))


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

# pylint: disable=C0103
parser = create_parser()
