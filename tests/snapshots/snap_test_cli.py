# -*- coding: utf-8 -*-
# snapshottest: v1 - https://goo.gl/zC4yUc
from __future__ import unicode_literals

from snapshottest import Snapshot


snapshots = Snapshot()

snapshots['TestCliCommands::test_ls 1'] = '''-----------------foobar (ACTIVE)------------------
items: 0 (0 bytes)
Read: 0/∞  Write: 0/∞
id STRING HASH KEY, range NUMBER RANGE KEY
foo STRING INDEX KEY
GLOBAL ALL INDEX bar-index
  items: 0 (0 bytes)
  Read: 0/∞  Write: 0/∞
  bar STRING HASH KEY
'''
