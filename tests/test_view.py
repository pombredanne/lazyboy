# -*- coding: utf-8 -*-
#
# © 2009 Digg, Inc. All rights reserved.
# Author: Ian Eure <ian@digg.com>
#
"""Unit tests for Lazyboy views."""

import unittest
import uuid
from itertools import islice

from cassandra.ttypes import Column, ColumnOrSuperColumn

from lazyboy.view import View
from lazyboy.key import Key
from lazyboy.record import Record
from test_record import MockClient


class ViewTest(unittest.TestCase):
    """Unit tests for Lazyboy views."""

    def setUp(self):
        """Prepare the text fixture."""
        obj = View()
        obj.key = Key(keyspace='eggs', column_family='bacon',
                      key='dummy_view')
        obj.record_key = Key(keyspace='spam', column_family='tomato')
        obj._get_cas = lambda: MockClient(['localhost:1234'])
        self.object = obj

    def test_keys_types(self):
        """Ensure that View._keys() returns the correct type & keys."""
        view = self.object
        keys = view._keys()
        self.assert_(hasattr(keys, '__iter__'))
        for key in tuple(keys):
            self.assert_(isinstance(key, Key))
            self.assert_(key.keyspace == view.record_key.keyspace)
            self.assert_(key.column_family == view.record_key.column_family)

    def __base_view_test(self, view, ncols, chunk_size):
        """Base test for View._keys iteration."""
        cols = map(ColumnOrSuperColumn,
                   [Column(name=x, value=x)
                    for x in range(ncols)])

        def get_slice(instance, keyspace, key, parent, predicate, level):
            try:
                start = int(predicate.slice_range.start)
            except ValueError:
                start = 0
            count = int(predicate.slice_range.count)
            return cols[start:start+count]

        view.chunk_size = chunk_size
        MockClient.get_slice = get_slice
        keys = tuple(view._keys())
        self.assert_(len(keys) == len(cols),
                     "Got %s keys instead of of %s" %
                     (len(keys), len(cols)))

        self.assert_(len(set(keys)) == len(keys),
                     "Duplicates present in output")

    def test_empty_view(self):
        """Make sure empty views work correctly."""
        self.__base_view_test(self.object, 0, 10)

    def test_view_even(self):
        """Make sure iteration works across boundaries ending on the
        chunk size."""
        self.__base_view_test(self.object, 100, 10)
        self.__base_view_test(self.object, 100, 100)
        self.__base_view_test(self.object, 10, 9)
        self.__base_view_test(self.object, 9, 10)

    def test_view_odd(self):
        """Make sure iteration works with an odd chunk_size resulting
        in a remainder."""
        self.__base_view_test(self.object, 100, 7)

    def test_iter(self):
        """Test View.__iter__()"""
        class FakeKey(Key):
            pass

        class FakeRecord(Record):
            def load(self, key):
                assert isinstance(key, FakeKey)
                self.key = key
                return self


        view = self.object
        view.record_class = FakeRecord
        keys = [FakeKey(keyspace="eggs", column_family="bacon", key=x)
                              for x in range(10)]
        view._keys = lambda: keys

        for record in view:
            self.assert_(isinstance(record, FakeRecord))
            self.assert_(isinstance(record.key, FakeKey))
            self.assert_(record.key in keys)

    def test_append(self):
        view = self.object
        MockClient.insert = lambda conn, keyspace, key, path, value, time, x: True
        rec = Record()
        rec.key = Key(keyspace="eggs", column_family="bacon", key="tomato")
        self.object.append(rec)


if __name__ == '__main__':
   unittest.main()
