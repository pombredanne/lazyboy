# -*- coding: utf-8 -*-
#
# © 2009, 2010 Digg, Inc. All rights reserved.
# Author: Ian Eure <ian@digg.com>
#

"""Lazyboy, an object-non-relational-manager for Cassandra."""

from lazyboy.connection import add_pool, get_pool
from lazyboy.key import Key
from lazyboy.record import Record, MirroredRecord
from lazyboy.recordset import RecordSet, KeyRecordSet
from lazyboy.view import (View, PartitionedView, BatchLoadingView,
                          FaultTolerantView)
from lazyboy.iterators import slice_iterator, sparse_get, sparse_multiget, \
    key_range, key_range_iterator, pack, unpack, multigetterator
from . import column_crud
from . import exceptions
