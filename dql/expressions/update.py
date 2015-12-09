""" Update expressions """
from __future__ import unicode_literals

from .base import Expression, Field, Value
from dql.util import resolve


def field_or_value(clause):
    """
    For a clause that could be a field or value,
    create the right one and return it

    """
    if hasattr(clause, 'getName') and clause.getName() != 'field':
        if clause.getName() == 'set_function':
            return SetFunction.from_clause(clause)
        else:
            return Value(resolve(clause))
    else:
        return Field(clause)


class UpdateExpression(Expression):

    """ Entry point for Update expressions """

    def __init__(self, expressions):
        self.expressions = expressions

    @classmethod
    def from_update(cls, update):
        """ Factory for creating an Update expression """
        expressions = []
        if update.set_expr:
            expressions.append(UpdateSetMany.from_clause(update.set_expr))
        if update.remove_expr:
            expressions.append(UpdateRemove.from_clause(update.remove_expr))
        if update.add_expr:
            expressions.append(UpdateAdd.from_clause(update.add_expr))
        if update.delete_expr:
            expressions.append(UpdateDelete.from_clause(update.delete_expr))
        return cls(expressions)

    def build(self, visitor):
        return ' '.join(e.build(visitor) for e in self.expressions)


class UpdateSetMany(Expression):

    """ Expression fragment for multiple set statements """

    def __init__(self, updates):
        super(UpdateSetMany, self).__init__()
        self.updates = updates

    @classmethod
    def from_clause(cls, clause):
        """ Factory method """
        updates = [UpdateSetOne.from_clause(subclause) for subclause in clause]
        return cls(updates)

    def build(self, visitor):
        return 'SET ' + ', '.join([u.build(visitor) for u in self.updates])


class SetFunction(Expression):

    """
    Expression fragment for a function used in a SET statement

    e.g. if_not_exists(field, value)

    """

    def __init__(self, fn_name, value1, value2):
        self.fn_name = fn_name
        self.value1 = value1
        self.value2 = value2

    @classmethod
    def from_clause(cls, clause):
        """ Factory method """
        return cls(clause[0], field_or_value(clause[1]),
                   field_or_value(clause[2]))

    def build(self, visitor):
        return (self.fn_name + '(' + self.value1.build(visitor) + ', ' +
                self.value2.build(visitor) + ')')


class UpdateSetOne(Expression):

    """ Expression fragment for a single SET statement """

    def __init__(self, field, value1, op=None, value2=None):
        self.field = field
        self.value1 = value1
        self.op = op
        self.value2 = value2

    @classmethod
    def from_clause(cls, clause):
        """ Factory method """
        field = clause[0]
        value1 = field_or_value(clause[1])
        op = None
        value2 = None
        if len(clause) > 2:
            op = clause[2]
            value2 = field_or_value(clause[3])
        return cls(field, value1, op, value2)

    def build(self, visitor):
        field = visitor.get_field(self.field)
        ret = field + ' = ' + self.value1.build(visitor)
        if self.value2 is not None:
            ret += ' ' + self.op + ' ' + self.value2.build(visitor)
        return ret


class UpdateRemove(Expression):

    """ Expression fragment for a REMOVE statement """

    def __init__(self, fields):
        self.fields = fields

    @classmethod
    def from_clause(cls, clause):
        """ Factory method """
        return cls(clause.asList())

    def build(self, visitor):
        fields = ', '.join([visitor.get_field(f) for f in self.fields])
        return 'REMOVE ' + fields


class UpdateAdd(Expression):

    """ Expression fragment for an ADD statement """

    def __init__(self, updates):
        self.updates = updates

    @classmethod
    def from_clause(cls, clause):
        """ Factory method """
        return cls([FieldValue.from_clause(c) for c in clause])

    def build(self, visitor):
        fields = ', '.join([u.build(visitor) for u in self.updates])
        return 'ADD ' + fields


class UpdateDelete(Expression):

    """ Expression fragment for a DELETE statement """

    def __init__(self, updates):
        self.updates = updates

    @classmethod
    def from_clause(cls, clause):
        """ Factory method """
        return cls([FieldValue.from_clause(c) for c in clause])

    def build(self, visitor):
        fields = ', '.join([u.build(visitor) for u in self.updates])
        return 'DELETE ' + fields


class FieldValue(Expression):

    """ A field-value pair used in an expression """

    def __init__(self, field, value):
        self.field = field
        self.value = value

    @classmethod
    def from_clause(cls, clause):
        """ Factory method """
        return cls(clause[0], resolve(clause[1]))

    def build(self, visitor):
        return (visitor.get_field(self.field) + ' ' +
                visitor.get_value(self.value))
