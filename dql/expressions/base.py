""" Common utilities for all expressions """
import six

from .visitor import dummy_visitor


@six.python_2_unicode_compatible
class Expression(object):

    """ Base class for all expressions and expression fragments """

    def build(self, visitor):
        """ Build string expression, using the visitor to encode values """
        raise NotImplementedError

    def __str__(self):
        return self.build(dummy_visitor)


class Field(Expression):

    """ Wrapper for a field in an expression """

    def __init__(self, field):
        self.field = field

    def build(self, visitor):
        return visitor.get_field(self.field)

    def evaluate(self, item):
        """ Pull the field off the item """
        return item.get(self.field)


class Value(Expression):

    """ Wrapper for a value in an expression """

    def __init__(self, val):
        self.value = val

    def build(self, visitor):
        return visitor.get_value(self.value)

    def evaluate(self, item):
        """ Values evaluate to themselves regardless of the item """
        return self.value
