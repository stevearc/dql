""" Constraint expressions for selecting """
from __future__ import unicode_literals

from .base import Expression, Field, Value
from dql.util import resolve


def field_or_value(clause):
    """
    For a clause that could be a field or value,
    create the right one and return it

    """
    if hasattr(clause, 'getName') and clause.getName() != 'field':
        return Value(resolve(clause))
    else:
        return Field(clause)


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
