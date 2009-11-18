# -*- coding: utf-8 -*-
#
# © 2009 Digg, Inc. All rights reserved.
# Author: Ian Eure <ian@digg.com>
#
"""Unit tests for Lazyboy views."""

import unittest
import uuid
import types
from itertools import islice

from cassandra.ttypes import Column, ColumnOrSuperColumn

import lazyboy.view as view
from lazyboy.key import Key
from lazyboy.iterators import pack, unpack
from lazyboy.record import Record
from test_record import MockClient


class IterTimeTest(unittest.TestCase):

    """Test lazyboy.view time iteration functions."""

    def test_iter_time(self):
        """Test _iter_time."""
        iterator = view._iter_time(days=1)
        first = 0
        for x in range(10):
            val = iterator.next()
            self.assert_(val > first)

    def test_test_iter_days(self):
        """Test _iter_days."""
        iterator = view._iter_days()
        first = "19000101"
        for x in range(10):
            val = iterator.next()
            self.assert_(val > first)


class ViewTest(unittest.TestCase):
    """Unit tests for Lazyboy views."""

    def setUp(self):
        """Prepare the text fixture."""
        obj = view.View()
        obj.key = Key(keyspace='eggs', column_family='bacon',
                      key='dummy_view')
        obj.record_key = Key(keyspace='spam', column_family='tomato')
        self.client = MockClient(['localhost:1234'])
        obj._get_cas = lambda: self.client
        self.object = obj

    def test_repr(self):
        """Test view.__repr__."""
        self.assert_(isinstance(repr(self.object), str))

    def test_len(self):
        """Test View.__len__."""
        self.client.get_count = lambda *args: 99
        self.object.key = Key("Eggs", "Bacon")
        self.assert_(len(self.object) == 99)

    def test_keys_types(self):
        """Ensure that View._keys() returns the correct type & keys."""
        view = self.object
        keys = view._keys()
        self.assert_(hasattr(keys, '__iter__'))
        for key in tuple(keys):
            self.assert_(isinstance(key, Key))
            self.assert_(key.keyspace == view.record_key.keyspace)
            self.assert_(key.column_family == view.record_key.column_family)

        view._keys("foo", "bar")

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
            return cols[start:start + count]

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


        view_ = self.object
        view_.record_class = FakeRecord
        keys = [FakeKey(keyspace="eggs", column_family="bacon", key=x)
                              for x in range(10)]
        view_._keys = lambda: keys

        for record in view_:
            self.assert_(isinstance(record, FakeRecord))
            self.assert_(isinstance(record.key, FakeKey))
            self.assert_(record.key in keys)

    def test_append(self):
        view = self.object
        MockClient.insert = \
            lambda conn, keyspace, key, path, value, time, x: True
        rec = Record()
        rec.key = Key(keyspace="eggs", column_family="bacon", key="tomato")
        self.object.append(rec)


class FaultTolerantViewTest(unittest.TestCase):

    """Test suite for lazyboy.view.FaultTolerantView."""

    def test_iter(self):
        """Make sure FaultTolerantView.__iter__ ignores load errors."""

        class IntermittentFailureRecord(object):

            def load(self, key):
                if key % 2 == 0:
                    raise Exception("Failed to load")
                self.key = key
                return self

        ftv = view.FaultTolerantView()

        client = MockClient(['localhost:1234'])
        client.get_count = lambda *args: 99
        ftv._get_cas = lambda: client

        ftv.record_class = IntermittentFailureRecord
        ftv._keys = lambda: range(10)
        res = tuple(ftv)
        self.assert_(len(res) == 5)
        for record in res:
            self.assert_(record.key % 2 != 0)


class BatchLoadingViewTest(unittest.TestCase):

    """Test lazyboy.view.BatchLoadingView."""

    def test_init(self):
        self.object = view.BatchLoadingView()
        self.assert_(hasattr(self.object, 'chunk_size'))
        self.assert_(isinstance(self.object.chunk_size, int))

    def test_iter(self):
        mg = view.multigetterator

        self.object = view.BatchLoadingView(None, Key("Eggs", "Bacon"))
        self.object._keys = lambda: [Key("Eggs", "Bacon", x)
                                     for x in range(25)]

        columns = [Column(x, x * x) for x in range(10)]
        data = {'Digg': {'Users': {}}}
        for key in self.object._keys():
            data['Digg']['Users'][key] = iter(columns)

        try:
            view.multigetterator = lambda *args, **kwargs: data
            self.assert_(isinstance(self.object.__iter__(),
                                    types.GeneratorType))

            for record in self.object:
                self.assert_(isinstance(record, Record))
                self.assert_(hasattr(record, 'key'))
                self.assert_(record.key.keyspace == "Digg")
                self.assert_(record.key.column_family == "Users")
                self.assert_(record.key.key in self.object.keys())
                for x in range(10):
                    self.assert_(x in record)
                    self.assert_(record[x] == x * x)

        finally:
            view.multigetterator = mg


class PartitionedViewTest(unittest.TestCase):

    """Test lazyboy.view.PartitionedView."""

    view_class = view.PartitionedView

    def setUp(self):
        """Prepare the text fixture."""
        obj = view.PartitionedView()
        obj.view_key = Key(keyspace='eggs', column_family='bacon',
                      key='dummy_view')
        obj.view_class = view.View
        self.object = obj

    def test_partition_keys(self):
        """Test PartitionedView.partition_keys."""
        keys = self.object.partition_keys()
        self.assert_(isinstance(keys, list) or
                     isinstance(keys, tuple) or
                     isinstance(keys, types.GeneratorType))

    def test_get_view(self):
        """Test PartitionedView._get_view."""
        partition = self.object._get_view(self.object.view_key)
        self.assert_(isinstance(partition, self.object.view_class))

    def test_iter(self):
        """Test PartitionedView.__iter__."""
        keys = ['eggs', 'bacon', 'tomato', 'sausage', 'spam']
        self.object.partition_keys = lambda: keys
        self.object._get_view = lambda key: [key]
        gen = self.object.__iter__()
        for record in gen:
            self.assert_(record in keys)

    def test_append_view(self):
        """Test PartitionedView._append_view."""
        record = Record()
        self.object._get_view = lambda key: key
        data = (('one', 'two'),
                ['one', 'two'],
                iter(('one', 'two')))

        for data_set in data:
            self.object.partition_keys = lambda: data_set
            self.assert_(self.object._append_view(record) == "one")


if __name__ == '__main__':
    unittest.main()
