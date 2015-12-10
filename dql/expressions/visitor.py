""" Visitor classes for traversing expressions """
import re


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
        if self._fields:
            return self._fields

    @property
    def expression_values(self):
        """ Dict of encoded variable names to the variables """
        if self._values:
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
