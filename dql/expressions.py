""" Objects to model nested update/constraint expressions """
from __future__ import unicode_literals

import re

import six

from .util import resolve


FIELD_RE = re.compile(r'^[^\.\[]+')


class Visitor(object):

    """
    Visitor that replaces field names and values with encoded versions

    Parameters
    ----------
    reserved_words : set, optional
        Set of (uppercase) words that are reserved by DynamoDB. These are used
        when encoding field names. If None, will default to encoding all
        fields.

    """

    def __init__(self, reserved_words=None):
        self._reserved_words = reserved_words
        self._fields = {}
        self._field_to_key = {}
        self._values = {}
        self._next_value = 1
        self._next_field = 1

    def get_field(self, field):
        """
        Get the safe representation of a field name

        For example, since 'order' is reserved, it would encode it as '#f1'

        """
        name = FIELD_RE.findall(field)[0]
        remainder = field[len(name):]
        if self._reserved_words is not None:
            if name.upper() not in self._reserved_words:
                return field
        if name in self._field_to_key:
            return self._field_to_key[field] + remainder
        next_key = '#f%d' % self._next_field
        self._next_field += 1
        self._field_to_key[name] = next_key
        self._fields[next_key] = name
        return next_key + remainder

    def get_value(self, value):
        """ Replace variable names with placeholders (e.g. ':v1') """
        next_key = ':v%d' % self._next_value
        self._next_value += 1
        self._values[next_key] = value
        return next_key

    @property
    def attribute_names(self):
        """ Dict of encoded field names to original names """
        return self._fields

    @property
    def expression_values(self):
        """ Dict of encoded variable names to the variables """
        return self._values


class DummyVisitor(Visitor):

    """ No-op visitor for testing """

    def get_field(self, field):
        """ No-op """
        return field

    def get_value(self, value):
        """ No-op """
        return repr(value)

# pylint: disable=C0103
dummy_visitor = DummyVisitor()
# pylint: enable=C0103


@six.python_2_unicode_compatible
class Expression(object):

    """ Base class for all expressions and expression fragments """

    def build(self, visitor):
        """ Build string expression, using the visitor to encode values """
        raise NotImplementedError

    def __str__(self):
        return self.build(dummy_visitor)


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


class Field(Expression):

    """ Wrapper for a field in an expression """

    def __init__(self, field):
        self.field = field

    def build(self, visitor):
        return visitor.get_field(self.field)


class Value(Expression):

    """ Wrapper for a value in an expression """

    def __init__(self, val):
        self.value = val

    def build(self, visitor):
        return visitor.get_value(self.value)


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


class ConstraintExpression(Expression):

    """
    Base class and entry point for constraint expressions

    e.g. WHERE foo = 1

    """

    @classmethod
    def from_where(cls, where):
        """ Factory method for creating the top-level expression """
        if where.conjunction:
            return Conjunction.from_clause(where)
        else:
            return cls.from_clause(where[0])

    @classmethod
    def from_clause(cls, clause):
        """ Factory method """
        name = clause.getName()
        if name == 'not':
            cond = Invert(cls.from_clause(clause[1]))
        elif name == 'operator':
            cond = OperatorConstraint.from_clause(clause)
        elif name == 'conjunction' or clause.conjunction:
            cond = Conjunction.from_clause(clause)
        elif name == 'function':
            cond = FunctionConstraint.from_clause(clause)
        elif name == 'between':
            cond = BetweenConstraint.from_clause(clause)
        elif name == 'in':
            cond = InConstraint.from_clause(clause)
        else:
            raise SyntaxError("Unknown constraint type %r" % name)
        return cond

    def build(self, visitor):
        raise NotImplementedError

    def possible_hash_fields(self):
        """
        Set of field names this expression could possibly be selecting for the
        hash key of a query.

        Hash keys must be exactly specified with "hash_key = value"

        """
        field = self.hash_field
        if field is None:
            return set()
        return set([field])

    @property
    def hash_field(self):
        """ The field of the hash key this expression can select, if any """
        return None

    def possible_range_fields(self):
        """
        Set of field names this expression could possibly be selecting for the
        range key of a query.

        Range keys can use operations such as <, >, <=, >=, begins_with, etc.

        """
        field = self.range_field
        if field is None:
            return set()
        return set([field])

    @property
    def range_field(self):
        """ The field of the range key this expression can select, if any """
        return None

    def __repr__(self):
        return "Constraint(%s)" % self


class Invert(ConstraintExpression):

    """ Invert another constraint expression with NOT """

    def __init__(self, constraint):
        self.constraint = constraint

    def build(self, visitor):
        return 'NOT ' + self.constraint.build(visitor)


class Conjunction(ConstraintExpression):

    """ Use AND and OR to join 2 or more expressions """

    def __init__(self, pieces):
        if len(pieces) < 3 or len(pieces) % 2 != 1:
            raise SyntaxError("Invalid conjunction %r" % pieces)
        self.pieces = pieces

    @classmethod
    def and_(cls, constraints):
        """ Factory for a group AND """
        return cls._factory(constraints, 'AND')

    @classmethod
    def or_(cls, constraints):
        """ Factory for a group OR """
        return cls._factory(constraints, 'OR')

    @classmethod
    def _factory(cls, constraints, op):
        """ Factory for joining constraints with a single conjunction """
        pieces = []
        for i, constraint in enumerate(constraints):
            pieces.append(constraint)
            if i != len(constraints) - 1:
                pieces.append(op)
        return cls(pieces)

    @classmethod
    def from_clause(cls, clause):
        """ Factory method """
        pieces = []
        for i, part in enumerate(clause):
            if i % 2 == 1:
                pieces.append(part)
            else:
                pieces.append(ConstraintExpression.from_clause(part))
        return cls(pieces)

    def build(self, visitor):
        strings = []
        for i, part in enumerate(self.pieces):
            if i % 2 == 1:
                strings.append(part)
            else:
                strings.append(part.build(visitor))
        return '(' + ' '.join(strings) + ')'

    def _get_fields(self, attr):
        """ Get the hash/range fields of all joined constraints """
        ret = set()
        if 'OR' in self.pieces:
            return ret
        for i in range(0, len(self.pieces), 2):
            const = self.pieces[i]
            field = getattr(const, attr)
            if field is not None:
                ret.add(field)
        return ret

    def possible_hash_fields(self):
        return self._get_fields('hash_field')

    def possible_range_fields(self):
        return self._get_fields('range_field')

    def __bool__(self):
        return bool(self.pieces)

    def remove_index(self, index):
        """
        This one takes some explanation. When we do a query with a WHERE
        statement, it may end up being a scan and it may end up being a query.
        If it is a query, we need to remove the hash and range key constraints
        from the expression and return that as the query_constraints. The
        remaining constraints, if any, are returned as the filter_constraints.

        """
        # We can only be doing this if all of the joining ops are AND (no OR),
        # so we don't even need to worry about OR's
        query = []
        remainder = []
        for i in range(0, len(self.pieces), 2):
            const = self.pieces[i]
            if const.hash_field == index.hash_key:
                query.append(const)
            elif (index.range_key is not None and
                  const.range_field == index.range_key):
                query.append(const)
            else:
                remainder.append(const)
        if len(query) == 1:
            query_constraints = query[0]
        else:
            query_constraints = Conjunction.and_(query)
        if len(remainder) == 0:
            filter_constraints = None
        elif len(remainder) == 1:
            filter_constraints = remainder[0]
        else:
            filter_constraints = Conjunction.and_(remainder)
        return (
            query_constraints,
            filter_constraints
        )


class OperatorConstraint(ConstraintExpression):

    """ Constraint expression for operations, e.g. foo = 4 """

    def __init__(self, field, operator, value):
        self.field = field
        self.operator = operator
        self.value = value
        if self.operator == '!=':
            self.operator = '<>'

    @classmethod
    def from_clause(cls, clause):
        """ Factory method """
        (field, operator, val) = clause
        return cls(field, operator, field_or_value(val))

    def build(self, visitor):
        field = visitor.get_field(self.field)
        val = self.value.build(visitor)
        return field + ' ' + self.operator + ' ' + val

    @property
    def hash_field(self):
        if self.operator == '=' and isinstance(self.value, Value):
            return self.field

    @property
    def range_field(self):
        if self.operator != '<>' and isinstance(self.value, Value):
            return self.field

    def remove_index(self, index):
        """
        See :meth:`~dql.expressions.Conjunction.remove_index`.

        This is called if the entire WHERE expression is just a "hash_key =
        value". In this case, the query_constraints are just this constraint,
        and there are no filter_constraints.

        """
        return (self, None)


class FunctionConstraint(ConstraintExpression):

    """ Constraint for function expressions e.g. attribute_exists(field) """

    def __init__(self, fn_name, field, operand=None):
        self.fn_name = fn_name
        self.field = field
        self.operand = operand

    @classmethod
    def from_clause(cls, clause):
        """ Factory method """
        fn_name = clause[0]
        if fn_name == 'size':
            return SizeConstraint.from_clause(clause)
        elif fn_name == 'attribute_type':
            return TypeConstraint.from_clause(clause)
        else:
            fn_name = clause[0]
            field = clause[1]
            if len(clause) > 2:
                return cls(fn_name, field, resolve(clause[2]))
            else:
                return cls(fn_name, field)

    def build(self, visitor):
        field = visitor.get_field(self.field)
        string = self.fn_name + '(' + field
        if self.operand is not None:
            val = visitor.get_value(self.operand)
            string += ', ' + val
        return string + ')'

    @property
    def range_field(self):
        if self.fn_name == 'begins_with':
            return self.field


class TypeConstraint(FunctionConstraint):

    """ Constraint for attribute_type() function """

    @classmethod
    def from_clause(cls, clause):
        """ Factory method """
        return cls(clause[0], clause[1], clause[2])


class SizeConstraint(ConstraintExpression):

    """ Constraint expression for size() function """

    def __init__(self, field, operator, value):
        self.field = field
        self.operator = operator
        self.value = value
        if self.operator == '!=':
            self.operator = '<>'

    @classmethod
    def from_clause(cls, clause):
        """ Factory method """
        [_, field, operator, val] = clause
        return cls(field, operator, resolve(val))

    def build(self, visitor):
        field = visitor.get_field(self.field)
        val = visitor.get_value(self.value)
        return 'size(' + field + ') ' + self.operator + ' ' + val


class BetweenConstraint(ConstraintExpression):

    """ Constraint expression for BETWEEN low AND high """

    def __init__(self, field, low, high):
        self.field = field
        self.low = low
        self.high = high

    @classmethod
    def from_clause(cls, clause):
        """ Factory method """
        [field, low, high] = clause
        return cls(field, resolve(low), resolve(high))

    def build(self, visitor):
        field = visitor.get_field(self.field)
        low = visitor.get_value(self.low)
        high = visitor.get_value(self.high)
        return field + ' BETWEEN ' + low + ' AND ' + high

    @property
    def range_field(self):
        return self.field


class InConstraint(ConstraintExpression):

    """ Constraint expression for membership in a set """

    def __init__(self, field, values):
        self.field = field
        self.values = values

    @classmethod
    def from_clause(cls, clause):
        """ Factory method """
        [field, vals] = clause
        return cls(field, resolve(vals))

    def build(self, visitor):
        values = (visitor.get_value(v) for v in self.values)
        return self.field + ' IN (' + ', '.join(values) + ')'
