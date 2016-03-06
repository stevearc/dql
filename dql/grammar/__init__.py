""" DQL language parser """
from pyparsing import (delimitedList, Optional, Group, restOfLine, Keyword,
                       Suppress, ZeroOrMore, oneOf, StringEnd, CharsNotIn,
                       quotedString, OneOrMore, Regex, Word, printables, Combine)

from .common import (from_, table, var, value, table_key, into, type_, upkey,
                     set_, primitive, var_val, filename, function, number)
from .query import (selection, where, limit, scan_limit, if_exists,
                    if_not_exists, keys_in)


def create_throughput(variable=primitive):
    """ Create a throughput specification """
    return (Suppress(upkey('throughput') | upkey('tp')) + Suppress('(') +
            variable + Suppress(',') + variable + Suppress(')'))\
        .setResultsName('throughput')


def create_throttle():
    """ Create a THROTTLE statement """
    throttle_amount = ('*' | Combine(number + '%') | number)
    return Group(function('throttle', throttle_amount, throttle_amount,
                          caseless=True)).setResultsName('throttle')

# pylint: disable=W0104,W0106


def _query(cmd):
    """ Create the grammar for a scan/query """
    action = upkey(cmd).setResultsName('action')
    consist = upkey('consistent').setResultsName('consistent')
    order_by = (Suppress(upkey('order') + upkey('by')) + var)\
        .setResultsName('order_by')
    ordering = (upkey('desc') | upkey('asc')).setResultsName('order')
    save = (Suppress(upkey('save')) + filename)\
        .setResultsName('save_file')

    return (action + Optional(consist) + selection + from_ + table +
            Optional(keys_in | where) +
            Optional(using) +
            Optional(limit) +
            Optional(scan_limit) +
            Optional(order_by) +
            Optional(ordering) +
            Optional(throttle) +
            Optional(save))


def create_select():
    """ Create the grammar for the 'select' statement """
    return _query('select')


def create_scan():
    """ Create the grammar for the 'scan' statement """
    return _query('scan')


def _global_index():
    """ Create grammar for a global index declaration """
    var_and_type = (var + Optional(type_))
    global_dec = Suppress(upkey('global')) + index
    range_key_etc = (Suppress(',') + Group(throughput) |
                     Optional(Group(Suppress(',') + var_and_type)
                              .setResultsName('range_key')) +
                     Optional(Suppress(',') + include_vars) +
                     Optional(Group(Suppress(',') + throughput)))
    global_spec = (Suppress('(') + primitive +
                   Suppress(',') + Group(var_and_type)
                   .setResultsName('hash_key') +
                   range_key_etc +
                   Suppress(')'))
    return Group(global_dec + global_spec).setName('global index')


def create_create():
    """ Create the grammar for the 'create' statement """
    create = upkey('create').setResultsName('action')
    hash_key = Group(upkey('hash') +
                     upkey('key'))
    range_key = Group(upkey('range') +
                      upkey('key'))

    local_index = Group(index + Suppress('(') + primitive +
                        Optional(Suppress(',') + include_vars) + Suppress(')'))
    index_type = (hash_key | range_key | local_index)\
        .setName('index specification').setResultsName('index')

    attr_declaration = Group(var.setResultsName('name') + type_ +
                             Optional(index_type))\
        .setName('attr').setResultsName('attr')
    attrs_declaration = (Suppress('(') +
                         Group(delimitedList(attr_declaration))
                         .setName('attrs').setResultsName('attrs') +
                         Optional(Suppress(',') + throughput) + Suppress(')'))

    global_index = _global_index()
    global_indexes = Group(OneOrMore(global_index))\
        .setResultsName('global_indexes')

    return (create + table_key + Optional(if_not_exists) + table +
            attrs_declaration + Optional(global_indexes))


def create_delete():
    """ Create the grammar for the 'delete' statement """
    delete = upkey('delete').setResultsName('action')
    return (
        delete +
        from_ +
        table +
        Optional(keys_in) +
        Optional(where) +
        Optional(using) +
        Optional(throttle)
    )


def create_insert():
    """ Create the grammar for the 'insert' statement """
    insert = upkey('insert').setResultsName('action')

    # VALUES
    attrs = Group(delimitedList(var)).setResultsName('attrs')
    value_group = Group(Suppress('(') + delimitedList(value) + Suppress(')'))
    values = Group(delimitedList(value_group)).setResultsName('list_values')
    values_insert = (Suppress('(') + attrs + Suppress(')') + upkey('values') +
                     values)

    # KEYWORDS
    keyword = Group(var + Suppress('=') + value)
    item = Group(Suppress('(') + delimitedList(keyword) + Suppress(')'))
    keyword_insert = delimitedList(item).setResultsName('map_values')

    return (insert + into + table + (values_insert | keyword_insert) + Optional(throttle))


def create_drop():
    """ Create the grammar for the 'drop' statement """
    drop = upkey('drop').setResultsName('action')
    return (drop + table_key + Optional(if_exists) + table)


def _create_update_expression():
    """ Create the grammar for an update expression """
    ine = (Word('if_not_exists') + Suppress('(') + var +
           Suppress(',') + var_val + Suppress(')'))
    list_append = (Word('list_append') + Suppress('(') + var_val +
                   Suppress(',') + var_val + Suppress(')'))
    fxn = Group(ine | list_append).setResultsName('set_function')
    # value has to come before var to prevent parsing TRUE/FALSE as variables
    path = (value | fxn | var)
    set_val = ((path + oneOf('+ -') + path) | path)
    set_cmd = Group(var + Suppress('=') + set_val)
    set_expr = (Suppress(upkey('set')) +
                delimitedList(set_cmd)).setResultsName('set_expr')
    add_expr = (Suppress(upkey('add')) +
                delimitedList(Group(var + value)))\
        .setResultsName('add_expr')
    delete_expr = (Suppress(upkey('delete')) +
                   delimitedList(Group(var + value)))\
        .setResultsName('delete_expr')
    remove_expr = (
        Suppress(
            upkey('remove')) +
        delimitedList(var)).setResultsName('remove_expr')
    return OneOrMore(set_expr | add_expr | delete_expr | remove_expr)\
        .setResultsName('update')


def create_update():
    """ Create the grammar for the 'update' statement """
    update = upkey('update').setResultsName('action')
    returns, none, all_, updated, old, new = \
        map(upkey, ['returns', 'none', 'all', 'updated', 'old',
                    'new'])
    return_ = returns + Group(none |
                              (all_ + old) |
                              (all_ + new) |
                              (updated + old) |
                              (updated + new))\
        .setResultsName('returns')
    return (
        update + table + update_expr +
        Optional(keys_in) +
        Optional(where) +
        Optional(using) +
        Optional(return_) +
        Optional(throttle)
    )


def create_alter():
    """ Create the grammar for the 'alter' statement """
    alter = upkey('alter').setResultsName('action')
    prim_or_star = (primitive | '*')

    set_throughput = (
        Suppress(upkey('set')) +
        Optional(Suppress(upkey('index')) + var.setResultsName('index')) +
        create_throughput(prim_or_star))

    drop_index = (Suppress(upkey('drop') + upkey('index')) + var + Optional(if_exists))\
        .setResultsName('drop_index')
    global_index = _global_index()
    create_index = (Suppress(upkey('create')) +
                    global_index.setResultsName('create_index') + Optional(if_not_exists))

    return (alter + table_key + table +
            (set_throughput | drop_index | create_index))


def create_dump():
    """ Create the grammar for the 'dump' statement """
    dump = upkey('dump').setResultsName('action')
    return (dump + upkey('schema') +
            Optional(Group(delimitedList(table)).setResultsName('tables')))


def create_load():
    """ Create the grammar for the 'load' statement """
    load = upkey('load').setResultsName('action')
    return (load + Group(filename).setResultsName('load_file') +
            upkey('into') + table + Optional(throttle))


def create_parser():
    """ Create the language parser """
    select = create_select()
    scan = create_scan()
    delete = create_delete()
    update = create_update()
    insert = create_insert()
    create = create_create()
    drop = create_drop()
    alter = create_alter()
    dump = create_dump()
    load = create_load()
    base = (select | scan | delete | update | insert |
            create | drop | alter | dump | load)
    explain = (upkey('explain').setResultsName('action') +
               Group(select | scan | delete | update |
                     insert | create | drop | alter))
    analyze = (upkey('analyze').setResultsName('action') +
               Group(select | scan | delete | update | insert))
    dql = (explain | analyze | base)
    dql.ignore('--' + restOfLine)
    return dql

# pylint: disable=C0103
using = (upkey('using') + var).setResultsName('using')
throughput = create_throughput()
throttle = create_throttle()
index = Group(Optional(upkey('all') | upkey('keys') | upkey('include')) +
              upkey('index')).setResultsName('index_type')
include_vars = Group(Suppress('[') + delimitedList(primitive) +
                     Suppress(']')).setResultsName('include_vars')
update_expr = _create_update_expression()
_statement = create_parser()
statement_parser = _statement + Suppress(';' | StringEnd())
parser = (Group(_statement) + ZeroOrMore(Suppress(';') + Group(_statement)) +
          Suppress(';' | StringEnd()))
line_parser = OneOrMore(ZeroOrMore(CharsNotIn(';')) + ';') + StringEnd()
