""" Common use grammars """
from pyparsing import (Word, Upcase, Optional, Combine, Group, alphas, nums,
                       alphanums, oneOf, quotedString, Keyword)


# pylint: disable=C0103
and_ = Upcase(Keyword("and", caseless=True))
from_ = Upcase(Keyword("from", caseless=True))
into = Upcase(Keyword("into", caseless=True))
table_key = Upcase(Keyword("table", caseless=True))

var = Word(alphas, alphanums + "_").setName("variable")
table = var.setResultsName('table')
type_ = (Upcase(Keyword('string', caseless=True)) |
         Upcase(Keyword('number', caseless=True)) |
         Upcase(Keyword('binary', caseless=True)))\
    .setName('type').setResultsName('type')

op = oneOf("= != < > >= <=", caseless=True).setName('operator')
_sign = Word("+-", exact=1)
num = Combine(Optional(_sign) + Word(nums) +
              Optional("." + Optional(Word(nums)))).setName('number')

value = Group(num.setResultsName('number') |
              quotedString.setResultsName('str') |
              var.setResultsName('idendifier')).setName('value')
