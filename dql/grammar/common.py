""" Common use grammars """
from pyparsing import (Word, Upcase, Optional, Combine, Group, alphas, nums,
                       alphanums, oneOf, quotedString, Keyword)


# pylint: disable=C0103
def upkey(name):
    """ Shortcut for creating an uppercase keyword """
    return Upcase(Keyword(name, caseless=True))

and_, from_, into, in_, table_key, null, where_, set_ = \
    map(upkey, ['and', 'from', 'into', 'in', 'table', 'null', 'where', 'set'])

var = Word(alphas, alphanums + '_-').setName('variable')
table = var.setResultsName('table')
type_ = (upkey('string') |
         upkey('number') |
         upkey('binary'))\
    .setName('type').setResultsName('type')

op = oneOf('= != < > >= <=', caseless=True).setName('operator')
_sign = Word('+-', exact=1)
num = Combine(Optional(_sign) + Word(nums) +
              Optional('.' + Optional(Word(nums)))).setName('number')

value = Group(null.setResultsName('null') |
              num.setResultsName('number') |
              quotedString.setResultsName('str') |
              var.setResultsName('idendifier')).setName('value')
