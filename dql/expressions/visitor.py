""" Visitor classes for traversing expressions """
import re


FIELD_RE = re.compile(r'\w+(?![^\[]*\])')


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
        return FIELD_RE.sub(self._maybe_replace_path, field)

    def _maybe_replace_path(self, match):
        """ Regex replacement method that will sub paths when needed """
        path = match.group(0)
        if self._reserved_words is None or path.upper() in self._reserved_words:
            return self._replace_path(path)
        else:
            return path

    def _replace_path(self, path):
        """ Get the replacement value for a path """
        if path in self._field_to_key:
            return self._field_to_key[path]
        next_key = '#f%d' % self._next_field
        self._next_field += 1
        self._field_to_key[path] = next_key
        self._fields[next_key] = path
        return next_key

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
