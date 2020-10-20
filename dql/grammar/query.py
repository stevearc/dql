""" Grammars for parsing query strings """
from pyparsing import (
    Forward,
    Group,
    Keyword,
    OneOrMore,
    Optional,
    Suppress,
    ZeroOrMore,
    delimitedList,
    infixNotation,
    nestedExpr,
    oneOf,
    opAssoc,
)

from dql.expressions.base import Field, Value
from dql.expressions.constraint import (
    BetweenConstraint,
    Conjunction,
    FunctionConstraint,
    InConstraint,
    Invert,
    OperatorConstraint,
    SizeConstraint,
    TypeConstraint,
)

from .common import (
    and_,
    and_or,
    function,
    integer,
    not_,
    or_,
    quoted,
    set_,
    string,
    types,
    upkey,
    value,
    var,
    var_val,
)
from .parsed_primitives import primitive as parsed_primitive
from .parsed_primitives import set_ as parsed_set_
from .parsed_primitives import string as parsed_string
from .parsed_primitives import value as parsed_value


def select_functions(expr):
    """ Create the function expressions for selection """
    body = Group(expr)
    return Group(
        function("timestamp", body, caseless=True)
        | function("ts", body, caseless=True)
        | function("utctimestamp", body, caseless=True)
        | function("utcts", body, caseless=True)
        | function("now", caseless=True)
        | function("utcnow", caseless=True)
    ).setResultsName("function")


def create_selection():
    """ Create a selection expression """
    operation = Forward()
    nested = Group(Suppress("(") + operation + Suppress(")")).setResultsName("nested")
    select_expr = Forward()
    functions = select_functions(select_expr)
    maybe_nested = functions | nested | Group(var_val)
    operation <<= maybe_nested + OneOrMore(oneOf("+ - * /") + maybe_nested)
    select_expr <<= operation | maybe_nested
    alias = Group(Suppress(upkey("as")) + var).setResultsName("alias")
    full_select = Group(
        Group(select_expr).setResultsName("selection") + Optional(alias)
    )
    return Group(
        Keyword("*") | upkey("count(*)") | delimitedList(full_select)
    ).setResultsName("attrs")


# pylint: disable=C0103
field_or_value = Group(parsed_value).setParseAction(lambda x: Value(x[0][0])).setName(
    "value"
) | Group(var).setName("field").setParseAction(lambda x: Field(x[0][0]))
var_or_quoted_var = var | quoted(var)


def create_query_constraint():
    """ Create a constraint for a query WHERE clause """
    op = oneOf("= < > >= <= != <>", caseless=True).setName("operator")
    basic_constraint = (var + op + field_or_value).setParseAction(
        OperatorConstraint.from_parser
    )
    between = (
        var
        + Suppress(upkey("between"))
        + parsed_primitive
        + Suppress(and_)
        + parsed_primitive
    ).setParseAction(BetweenConstraint.from_parser)
    is_in = (var + Suppress(upkey("in")) + parsed_set_).setParseAction(
        InConstraint.from_parser
    )
    fxn = (
        function("attribute_exists", var_or_quoted_var)
        | function("attribute_not_exists", var_or_quoted_var)
        | function("begins_with", var_or_quoted_var, parsed_value)
        | function("contains", var_or_quoted_var, parsed_value)
    ).setParseAction(FunctionConstraint.from_parser)
    size_fxn = (function("size", var_or_quoted_var) + op + parsed_value).setParseAction(
        SizeConstraint.from_parser
    )
    type_fxn = function("attribute_type", var_or_quoted_var, types).setParseAction(
        TypeConstraint.from_parser
    )
    return (between | basic_constraint | is_in | fxn | size_fxn | type_fxn).setName(
        "constraint"
    )


# pylint: disable=C0103
constraint = create_query_constraint()


def create_where():
    """ Create a grammar for the 'where' clause used by 'select' """
    full_constraint = infixNotation(
        constraint,
        [
            (not_, 1, opAssoc.RIGHT, Invert.from_parser),
            (and_, 2, opAssoc.LEFT, Conjunction.from_parser),
            (or_, 2, opAssoc.LEFT, Conjunction.from_parser),
        ],
    )
    return upkey("where") + full_constraint.setResultsName("where")


def create_keys_in():
    """ Create a grammer for the 'KEYS IN' clause used for queries """
    keys = Group(
        Optional(Suppress("("))
        + value
        + Optional(Suppress(",") + value)
        + Optional(Suppress(")"))
    )
    return (Suppress(upkey("keys") + upkey("in")) + delimitedList(keys)).setResultsName(
        "keys_in"
    )


# pylint: disable=C0103
if_exists = Group(upkey("if") + upkey("exists")).setResultsName("exists")
if_not_exists = Group(upkey("if") + upkey("not") + upkey("exists")).setResultsName(
    "not_exists"
)

where = create_where()
keys_in = create_keys_in()
limit = Group(upkey("limit") + Group(integer)).setResultsName("limit")
scan_limit = Group(upkey("scan") + upkey("limit") + Group(integer)).setResultsName(
    "scan_limit"
)
selection = create_selection()
