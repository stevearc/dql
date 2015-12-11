""" Selection expressions """
from __future__ import unicode_literals

import six
from datetime import datetime
from dateutil.parser import parse
from dateutil.tz import tzlocal, tzutc

from .base import Expression, Field, Value
from .visitor import dummy_visitor
from dql.util import resolve


try:
    from collections import OrderedDict
except ImportError:  # pragma: no cover
    from ordereddict import OrderedDict  # pylint: disable=F0401


def add(a, b):
    """ Add two values, ignoring None """
    if a is None:
        if b is None:
            return None
        else:
            return b
    elif b is None:
        return a
    return a + b


def sub(a, b):
    """ Subtract two values, ignoring None """
    if a is None:
        if b is None:
            return None
        else:
            return -1 * b
    elif b is None:
        return a
    return a - b


def mul(a, b):
    """ Multiply two values, ignoring None """
    if a is None:
        if b is None:
            return None
        else:
            return b
    elif b is None:
        return a
    return a * b


def div(a, b):
    """ Divide two values, ignoring None """
    if a is None:
        if b is None:
            return None
        else:
            return 1 / b
    elif b is None:
        return a
    return a / b


OP_MAP = {
    '+': add,
    '-': sub,
    '*': mul,
    '/': div,
}


def parse_expression(clause):
    """ For a clause that could be a field, value, or expression """
    if isinstance(clause, Expression):
        return clause
    elif hasattr(clause, 'getName') and clause.getName() != 'field':
        if clause.getName() == 'nested':
            return AttributeSelection.from_statement(clause)
        elif clause.getName() == 'function':
            return SelectFunction.from_statement(clause)
        else:
            return Value(resolve(clause[0]))
    else:
        return Field(clause[0])


@six.python_2_unicode_compatible
class SelectionExpression(Expression):

    """ Entry point for Selection expressions """

    def __init__(self, expressions, is_count=False):
        self.expressions = expressions
        self.is_count = is_count
        self._all_fields = None

    def convert(self, item, sanitize=False):
        """ Convert an item into an OrderedDict with the selected fields """
        if not self.expressions:
            return item
        ret = OrderedDict()
        for expr in self.expressions:
            expr.populate(item, ret, sanitize)
        return ret

    @classmethod
    def from_selection(cls, selection):
        """ Factory for creating a Selection expression """
        expressions = []
        # Have to special case the '*' and 'COUNT(*)' selections
        if selection[0] == '*':
            return cls(expressions)
        elif selection[0] == 'COUNT(*)':
            return cls(expressions, True)
        for attr in selection:
            name = attr.getName()
            if name == 'selection':
                expr = NamedExpression.from_statement(attr)
            else:
                raise SyntaxError("Unknown selection name: %r for %s" % (name, attr))
            expressions.append(expr)
        return cls(expressions)

    def build(self, visitor):
        fields = set()
        for expr in self.expressions:
            fields.update(expr.build(visitor))
        if fields:
            return fields

    @property
    def all_fields(self):
        """ A set of all fields that are required by this statement """
        if self._all_fields is None:
            self._all_fields = self.build(dummy_visitor)
        return self._all_fields

    @property
    def all_keys(self):
        """ The keys, in order, that are selected by the statement """
        return [e.key for e in self.expressions]

    def __str__(self):
        return ' '.join(str(e) for e in self.expressions)


@six.python_2_unicode_compatible
class NamedExpression(Expression):
    """ Wrapper around AttributeSelection that holds the alias (if any) """

    def __init__(self, expr, alias=None):
        self.expr = expr
        self.alias = alias

    @classmethod
    def from_statement(cls, statement):
        """ Parse the selection expression and alias from a statement """
        alias = None
        if statement.alias:
            alias = statement.alias[0]
        return cls(AttributeSelection.from_statement(statement[0]), alias)

    @property
    def key(self):
        """ The name that this will occupy in the final result dict """
        if self.alias:
            return self.alias
        else:
            return str(self.expr)

    def populate(self, item, ret, sanitize):
        """ Evaluate the child expression and put result into return value """
        value = self.expr.evaluate(item)
        if sanitize and isinstance(value, TypeError):
            return
        ret[self.key] = value

    def build(self, visitor):
        return self.expr.build(visitor)

    def __str__(self):
        base = str(self.expr)
        if self.alias is not None:
            base += " AS " + self.alias
        return base


@six.python_2_unicode_compatible
class AttributeSelection(Expression):
    """ A tree of select expressions """

    def __init__(self, expr1, op=None, expr2=None):
        self.expr1 = expr1
        self.op = op
        self.expr2 = expr2

    @classmethod
    def from_statement(cls, statement):
        """ Factory for creating a Attribute expression """
        components = list(statement)
        if len(components) == 1:
            return cls(parse_expression(components[0]))
        while len(components) > 3:
            replaced = False
            for i in range(1, len(components), 2):
                if components[i] in ['/', '*']:
                    components[i - 1:i + 2] = [AttributeSelection.from_statement(components[i - 1:i + 2])]
                    replaced = True
                    break
            if not replaced:
                components[:3] = [AttributeSelection.from_statement(components[:3])]
        return cls(parse_expression(components[0]), components[1], parse_expression(components[2]))

    def build(self, visitor):
        fields = set()
        for expr in [self.expr1, self.expr2]:
            if expr is None:
                pass
            elif isinstance(expr, Field):
                fields.add(expr.build(visitor))
            elif isinstance(expr, (AttributeSelection, SelectFunction)):
                fields.update(expr.build(visitor))
        return fields

    def evaluate(self, item):
        """ Evaluate this expression for a partiular item """
        if self.expr2 is None:
            return self.expr1.evaluate(item)
        v1, v2 = self.expr1.evaluate(item), self.expr2.evaluate(item)
        try:
            return OP_MAP[self.op](v1, v2)
        except TypeError as e:
            return e

    def __str__(self):
        if self.expr2 is None:
            return str(self.expr1)
        return "(%s %s %s)" % (self.expr1, self.op, self.expr2)


class SelectFunction(Expression):
    """ Base class for special select functions """

    @classmethod
    def from_statement(cls, statement):
        """ Create a selection function from a statement """
        if statement[0] in ['TS', 'TIMESTAMP', 'UTCTIMESTAMP', 'UTCTS']:
            return TimestampFunction.from_statement(statement)
        elif statement[0] in ['NOW', 'UTCNOW']:
            return NowFunction.from_statement(statement)
        else:
            raise SyntaxError("Unknown function %r" % statement[0])

    def evaluate(self, item):
        """ Evaluate this expression for a partiular item """
        raise NotImplementedError


@six.python_2_unicode_compatible
class NowFunction(SelectFunction):
    """ Function to grab the current time """

    def __init__(self, utc):
        self.utc = utc

    @classmethod
    def from_statement(cls, statement):
        return cls(statement[0] == 'UTCNOW')

    def __str__(self):
        if self.utc:
            return 'UTCNOW()'
        else:
            return 'NOW()'

    def build(self, visitor):
        return []

    def evaluate(self, item):
        if self.utc:
            return datetime.utcnow().replace(tzinfo=tzutc())
        else:
            return datetime.now().replace(tzinfo=tzlocal())


@six.python_2_unicode_compatible
class TimestampFunction(SelectFunction):
    """ Function that parses a field or literal as a datetime """

    def __init__(self, expr, utc):
        self.expr = expr
        self.utc = utc

    @classmethod
    def from_statement(cls, statement):
        expr = AttributeSelection.from_statement(statement[1])
        utc = statement[0] in ['UTCTIMESTAMP', 'UTCTS']
        return cls(expr, utc)

    def __str__(self):
        if self.utc:
            base = 'UTCTIMESTAMP'
        else:
            base = 'TIMESTAMP'
        return base + "(%s)" % self.expr

    def build(self, visitor):
        return self.expr.build(visitor)

    def evaluate(self, item):
        base_value = self.expr.evaluate(item)
        if base_value is None:
            return None
        elif isinstance(base_value, six.string_types):
            dt = parse(base_value)
        else:
            base_value = float(base_value)
            if self.utc:
                meth = datetime.utcfromtimestamp
            else:
                meth = datetime.fromtimestamp
            try:
                dt = meth(base_value)
            except ValueError:
                dt = meth(base_value / 1000)
            except TypeError as e:
                return e
        if self.utc:
            return dt.replace(tzinfo=tzutc())
        else:
            return dt.replace(tzinfo=tzlocal())
