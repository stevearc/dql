""" DQL language parser """
# TODO: more complex WHERE clauses (str matching)
# TODO: select raw? (keyword for ResultSet instead of dicts?)
# TODO: multiple statements (in a file or on a line separated by ';')
from pyparsing import (CaselessLiteral, Word, Upcase, delimitedList, Optional,
                       Combine, Group, alphas, nums, alphanums, Forward, oneOf,
                       quotedString, ZeroOrMore, restOfLine, Keyword, LineEnd,
                       Suppress)


# pylint: disable=W0104,W0106

def create_parser():
    """ Create the language parser """
    select_stmt = Forward()
    delete_stmt = Forward()
    update_stmt = Forward()
    create_stmt = Forward()
    insert_stmt = Forward()
    drop_stmt = Forward()

    select = Upcase(Keyword("select", caseless=True)).setResultsName('action')
    delete = Upcase(Keyword("delete", caseless=True)).setResultsName('action')
    update = Upcase(Keyword("update", caseless=True)).setResultsName('action')
    create = Upcase(Keyword("create", caseless=True)).setResultsName('action')
    insert = Upcase(Keyword("insert", caseless=True)).setResultsName('action')
    drop = Upcase(Keyword("drop", caseless=True)).setResultsName('action')
    and_ = Upcase(Keyword("and", caseless=True))
    from_ = Upcase(Keyword("from", caseless=True))
    into = Upcase(Keyword("into", caseless=True))
    using = Upcase(Keyword('using', caseless=True))
    hash_key = Group(Upcase(Keyword("hash", caseless=True)) +
                     Upcase(Keyword('key', caseless=True)))
    range_key = Group(Upcase(Keyword("range", caseless=True)) +
                      Upcase(Keyword('key', caseless=True)))
    values_key = Upcase(Keyword("values", caseless=True))
    table_key = Upcase(Keyword("table", caseless=True))
    if_exists = Group(Upcase(Keyword('if', caseless=True)) +
                      Upcase(Keyword('exists', caseless=True)))\
        .setResultsName('exists')
    if_not_exists = Group(Upcase(Keyword('if', caseless=True)) +
                          Upcase(Keyword('not', caseless=True)) +
                          Upcase(Keyword('exists', caseless=True)))\
        .setResultsName('not_exists')

    var = Word(alphas, alphanums + "_").setName("variable")
    table = var.setResultsName('table')
    type_ = (Upcase(Keyword('string', caseless=True)) |
             Upcase(Keyword('number', caseless=True)) |
             Upcase(Keyword('binary', caseless=True)))\
        .setName('type').setResultsName('type')

    op = oneOf("= != < > >= <=", caseless=True).setName('operator')
    sign = Word("+-", exact=1)
    num = Combine(Optional(sign) + Word(nums) +
                  Optional("." + Optional(Word(nums)))).setName('number')

    value = Group(num.setResultsName('number') |
                  quotedString.setResultsName('str') |
                  var.setResultsName('idendifier')).setName('value')

    # WHERE
    where_exp = Forward()
    where_clause = Group(
        (var + op + value) |
        ("(" + where_exp + ")")
    )
    where_exp << where_clause + ZeroOrMore(and_ + where_clause)
    where = CaselessLiteral('where') + where_exp.setResultsName('where')

    # ATTRS
    attrs = Group(delimitedList(var)).setResultsName('attrs')

    # ATTR DECLARATION
    index = Group(Upcase(Keyword('index', caseless=True)) + Suppress('(') +
                  value + Suppress(')'))
    index_type = (hash_key | range_key | index)\
        .setName('index specification').setResultsName('index')
    attr_declaration = Group(var.setResultsName('name') + type_ + index_type)\
        .setName('attr').setResultsName('attr')
    attrs_declaration = Group(delimitedList(attr_declaration))\
        .setName('attrs').setResultsName('attrs')

    # VALUES
    value_group = Group(Suppress('(') + delimitedList(value) + Suppress(')'))
    values = Group(delimitedList(value_group)).setResultsName('data')

    # STATEMENTS
    # TODO: limit
    select_stmt << (select + from_ + table + where +
                    Optional(using + value).setResultsName('using'))
    delete_stmt << (delete + from_ + table + where +
                    Optional(using + value).setResultsName('using'))
    # TODO: update stmt
    update_stmt << (update + table + where)
    create_stmt << (create + table_key + Optional(if_not_exists) + table +
                    '(' + attrs_declaration + ')')
    insert_stmt << (insert + into + table + Suppress('(') +
                    attrs + Suppress(')') +
                    values_key + values)

    drop_stmt << (drop + table_key + Optional(if_exists) + table)

    dql = ((select_stmt | delete_stmt | update_stmt | create_stmt |
            insert_stmt | drop_stmt) + Suppress(LineEnd()))
    comment = "--" + restOfLine
    dql.ignore(comment)

    return dql

parser = create_parser()  # pylint: disable=C0103
