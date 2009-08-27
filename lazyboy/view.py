# -*- coding: utf-8 -*-
#
# © 2009 Digg, Inc. All rights reserved.
# Author: Ian Eure <ian@digg.com>
#
"""Lazyboy: Views."""

import time
import datetime
import hashlib

from cassandra.ttypes import ColumnPath, ColumnParent, \
    SlicePredicate, SliceRange, ConsistencyLevel

from lazyboy.key import Key
from lazyboy.base import CassandraBase
from lazyboy.record import Record
from lazyboy.connection import Client

def _iter_time(start=None, **kwargs):
    day = start or datetime.datetime.today()
    intv = datetime.timedelta(**kwargs)
    while day.year >= 1900:
        yield day.strftime('%Y%m%d')
        day = day - intv

def _iter_days(start = None):
    return self._iter_time(start, days=1)


class View(CassandraBase):
    """A regular view."""

    def __init__(self, view_key=None, record_key=None, record_class=None):
        CassandraBase.__init__(self)

        self.chunk_size = 100
        self.key = view_key
        self.record_key = record_key
        self.record_class = record_class or Record

    def __repr__(self):
        return "%s: %s" % (self.__class__.__name__, self.key)

    def _keys(self):
        """Return keys in the view."""
        client = self._get_cas()
        assert isinstance(client, Client), \
            "Incorrect client instance: %s" % client.__class__
        last_col = ""
        chunk_size = self.chunk_size
        passes = 0
        while True:
            fudge = int(passes > 0)
            cols = client.get_slice(
                self.key.keyspace, self.key.key, self.key,
                SlicePredicate(slice_range=SliceRange(
                        last_col, "", 0, chunk_size + fudge)),
                ConsistencyLevel.ONE)

            if len(cols) == 0:
                raise StopIteration()

            for obj in cols[fudge:]:
                col = obj.column
                yield self.record_key.clone(key=col.value)

            last_col = col.name
            passes += 1

            if len(cols) < self.chunk_size:
                raise StopIteration()

    def __iter__(self):
        """Iterate over all objects in this view."""
        return (self.record_class().load(key) for key in self._keys())

    def append(self, record):
        assert isinstance(record, Record), \
            "Can't append non-record type %s to view %s" % \
            (record.__class__, self.__class__)
        ts = record.timestamp()
        path = self.key.clone(column="%s.%s" % (str(ts), record.key.key))
        self._get_cas().insert(self.key.keyspace, self.key.key,
                               path, record.key.key, ts, 0)


class PartitionedView(object):
    """A Lazyboy view which is partitioned across rows."""
    def __init__(self, view_key=None, view_class=None):
        self.view_key = view_key
        self.view_class = view_class

    def partition_keys():
        """Return a sequence of row keys for the view partitions."""
        return ()

    def _get_view(self, key):
        """Return an instance of a view for a partition key."""
        return self.view_class(self.view_key.clone(key=key))

    def __iter__(self):
        """Iterate over records in the view."""
        for view in (self._get_view(key) for key in self.partition_keys()):
            for record in view:
                yield record

    def _append_view(self, record):
        """Return the view which this record should be appended to.

        This defaults to the first view from partition_keys, but you
        can partition by anything, e.g. first letter of some field in
        the record.
        """
        keys = self.partition_keys()
        if hasattr(keys, '__iter__'):
            key = keys.next()
            keys.close()
        else:
            key = keys[0]

        return self._get_view(key)

    def append(self, record):
        """Append a record to the view."""
        return self._append_view(record).append(record)
