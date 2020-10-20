""" Constraint expressions for selecting """
from decimal import Decimal
from typing import TYPE_CHECKING, Any, List, Optional, Set, Union

from .base import Expression, Field, Value

if TYPE_CHECKING:
    from .visitor import Visitor

numeric = Union[int, float, Decimal]


class ConstraintExpression(Expression):

    """
    Base class and entry point for constraint expressions

    e.g. WHERE foo = 1

    """

    def build(self, visitor: "Visitor") -> str:
        raise NotImplementedError

    def possible_hash_fields(self) -> Set[str]:
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
    def hash_field(self) -> Optional[str]:
        """ The field of the hash key this expression can select, if any """
        return None

    def possible_range_fields(self) -> Set[str]:
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
    def range_field(self) -> Optional[str]:
        """ The field of the range key this expression can select, if any """
        return None

    def __repr__(self) -> str:
        return "Constraint(%s)" % self

    def __ne__(self, other: Any) -> bool:
        return not (self == other)


class Invert(ConstraintExpression):

    """ Invert another constraint expression with NOT """

    def __init__(self, constraint: "ConstraintExpression"):
        self.constraint = constraint

    @classmethod
    def from_parser(cls, result):
        return cls(result[0][1])

    def build(self, visitor: "Visitor") -> str:
        return "NOT " + self.constraint.build(visitor)

    def __hash__(self) -> int:
        return hash(self.constraint) + 1

    def __eq__(self, other: Any) -> bool:
        return isinstance(other, Invert) and self.constraint == other.constraint


class Conjunction(ConstraintExpression):

    """ Use AND and OR to join 2 or more expressions """

    def __init__(self, is_and: bool, *args: "ConstraintExpression"):
        self.pieces = list(args)
        self.is_and = is_and

    @classmethod
    def from_parser(cls, result):
        result = result[0]
        conj = result[1]
        pieces = []
        is_and = conj == "AND"
        for i in range(0, len(result), 2):
            expr = result[i]
            if isinstance(expr, Conjunction) and is_and == expr.is_and:
                pieces.extend(expr.pieces)
            else:
                pieces.append(expr)
        return cls(is_and, *pieces)

    def build(self, visitor: "Visitor") -> str:
        delimiter = " AND " if self.is_and else " OR "
        return "(" + delimiter.join([p.build(visitor) for p in self.pieces]) + ")"

    def _get_fields(self, attr: str) -> Set[str]:
        """ Get the hash/range fields of all joined constraints """
        ret: Set[str] = set()
        if not self.is_and:
            return ret
        for const in self.pieces:
            field = getattr(const, attr)
            if field is not None:
                ret.add(field)
        return ret

    def possible_hash_fields(self) -> Set[str]:
        return self._get_fields("hash_field")

    def possible_range_fields(self) -> Set[str]:
        return self._get_fields("range_field")

    def __bool__(self) -> bool:
        return bool(self.pieces)

    def remove_index(self, index):
        """
        This one takes some explanation. When we do a query with a WHERE
        statement, it may end up being a scan and it may end up being a query.
        If it is a query, we need to remove the hash and range key constraints
        from the expression and return that as the query_constraints. The
        remaining constraints, if any, are returned as the filter_constraints.

        """
        assert self.is_and
        query = []
        remainder = []
        for const in self.pieces:
            if const.hash_field == index.hash_key:
                query.append(const)
            elif index.range_key is not None and const.range_field == index.range_key:
                query.append(const)
            else:
                remainder.append(const)
        if len(query) == 1:
            query_constraints = query[0]
        else:
            query_constraints = Conjunction(True, *query)
        if not remainder:
            filter_constraints = None
        elif len(remainder) == 1:
            filter_constraints = remainder[0]
        else:
            filter_constraints = Conjunction(True, *remainder)
        return (query_constraints, filter_constraints)

    def __hash__(self) -> int:
        return hash(self.is_and) + hash(self.pieces)

    def __eq__(self, other: Any) -> bool:
        return (
            isinstance(other, Conjunction)
            and self.is_and == other.is_and
            and self.pieces == other.pieces
        )


class OperatorConstraint(ConstraintExpression):

    """ Constraint expression for operations, e.g. foo = 4 """

    def __init__(self, field: str, operator: str, value: Union[Field, Value]):
        self.field = field
        self.operator = operator
        self.value = value
        if self.operator == "!=":
            self.operator = "<>"

    @classmethod
    def from_parser(cls, result):
        (field, operator, val) = result
        return cls(field, operator, val)

    def build(self, visitor: "Visitor") -> str:
        field = visitor.get_field(self.field)
        val = self.value.build(visitor)
        return field + " " + self.operator + " " + val

    @property
    def hash_field(self) -> Optional[str]:
        if self.operator == "=" and isinstance(self.value, Value):
            return self.field
        return None

    @property
    def range_field(self) -> Optional[str]:
        if self.operator != "<>" and isinstance(self.value, Value):
            return self.field
        return None

    def remove_index(self, index):
        """
        See :meth:`~dql.expressions.Conjunction.remove_index`.

        This is called if the entire WHERE expression is just a "hash_key =
        value". In this case, the query_constraints are just this constraint,
        and there are no filter_constraints.

        """
        return (self, None)

    def __hash__(self) -> int:
        return hash(self.field) + hash(self.operator) + hash(self.value)

    def __eq__(self, other: Any) -> bool:
        return (
            isinstance(other, OperatorConstraint)
            and self.field == other.field
            and self.operator == other.operator
            and self.value == other.value
        )


class FunctionConstraint(ConstraintExpression):

    """ Constraint for function expressions e.g. attribute_exists(field) """

    def __init__(self, fn_name: str, field: str, operand: Any = None):
        self.fn_name = fn_name
        self.field = field
        self.operand = operand

    @classmethod
    def from_parser(cls, result):
        fn_name, field = result[:2]
        operand = result[2] if len(result) == 3 else None
        return cls(fn_name, field, operand)

    def build(self, visitor: "Visitor") -> str:
        field = visitor.get_field(self.field)
        string = self.fn_name + "(" + field
        if self.operand is not None:
            val = visitor.get_value(self.operand)
            string += ", " + val
        return string + ")"

    @property
    def range_field(self) -> Optional[str]:
        if self.fn_name == "begins_with":
            return self.field
        return None

    def __hash__(self) -> int:
        return hash(self.fn_name) + hash(self.field) + hash(self.operand)

    def __eq__(self, other: Any) -> bool:
        return (
            isinstance(other, FunctionConstraint)
            and self.fn_name == other.fn_name
            and self.field == other.field
            and self.operand == other.operand
        )


class TypeConstraint(FunctionConstraint):

    """ Constraint for attribute_type() function """

    @classmethod
    def from_parser(cls, result):
        """ Factory method """
        return cls(*result)


class SizeConstraint(ConstraintExpression):

    """ Constraint expression for size() function """

    def __init__(self, field: str, operator: str, value: Any):
        self.field = field
        self.operator = operator
        self.value = value
        if self.operator == "!=":
            self.operator = "<>"

    @classmethod
    def from_parser(cls, clause):
        """ Factory method """
        [_, field, operator, val] = clause
        return cls(field, operator, val)

    def build(self, visitor: "Visitor") -> str:
        field = visitor.get_field(self.field)
        val = visitor.get_value(self.value)
        return "size(" + field + ") " + self.operator + " " + val

    def __hash__(self) -> int:
        return hash(self.field) + hash(self.operator) + hash(self.value)

    def __eq__(self, other: Any) -> bool:
        return (
            isinstance(other, SizeConstraint)
            and self.field == other.field
            and self.operator == other.operator
            and self.value == other.value
        )


class BetweenConstraint(ConstraintExpression):

    """ Constraint expression for BETWEEN low AND high """

    def __init__(self, field: str, low: numeric, high: numeric):
        self.field = field
        self.low = low
        self.high = high

    @classmethod
    def from_parser(cls, result):
        (field, low, high) = result
        return cls(field, low, high)

    def build(self, visitor: "Visitor") -> str:
        field = visitor.get_field(self.field)
        low = visitor.get_value(self.low)
        high = visitor.get_value(self.high)
        return field + " BETWEEN " + low + " AND " + high

    @property
    def range_field(self) -> str:
        return self.field

    def __hash__(self) -> int:
        return hash(self.field) + hash(self.low) + hash(self.high)

    def __eq__(self, other: Any) -> bool:
        return (
            isinstance(other, BetweenConstraint)
            and self.field == other.field
            and self.low == other.low
            and self.high == other.high
        )


class InConstraint(ConstraintExpression):

    """ Constraint expression for membership in a set """

    def __init__(self, field: str, values: List):
        self.field = field
        self.values = values

    @classmethod
    def from_parser(cls, result):
        field, vals = result
        return cls(field, vals)

    def build(self, visitor: "Visitor") -> str:
        values = (visitor.get_value(v) for v in self.values)
        field = visitor.get_field(self.field)
        return field + " IN (" + ", ".join(values) + ")"

    def __hash__(self) -> int:
        return hash(self.field) + sum(map(hash, self.values))

    def __eq__(self, other: Any) -> bool:
        return (
            isinstance(other, InConstraint)
            and self.field == other.field
            and set(self.values) == set(other.values)
        )
