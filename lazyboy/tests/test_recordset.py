# -*- coding: utf-8 -*-
#
# © 2009 Digg, Inc. All rights reserved.
# Author: Ian Eure <ian@digg.com>
#
"""Unit tests for Lazyboy recordset module."""

import unittest
import uuid
from random import randrange, sample
from operator import attrgetter
from functools import partial

from test_base import CassandraBaseTest
from test_record import MockClient, _last_cols, _inserts

from cassandra.ttypes import ColumnOrSuperColumn

from lazyboy.key import Key
from lazyboy.record import Record
import lazyboy.recordset as sets
#import valid, missing, modified, RecordSet, KeyRecordSet
from lazyboy.exceptions import ErrorMissingKey, ErrorMissingField


def rand_set(records):
    return sample(records, randrange(1, len(records)))


class TestFunctions(unittest.TestCase):

    """Unit tests for record utility functions."""

    def setUp(self):
        """Prepare the test environment."""
        num_records = 15
        keys = [Key(keyspace='spam', column_family='bacon')
                for x in range(num_records)]

        records = []
        for key in keys:
            r = Record()
            r.key = key
            records.append(r)

        self.records = records

    def test_valid(self):
        """Test lazyboy.recordset.valid."""
        self.assert_(sets.valid(self.records))
        invalid = rand_set(self.records)
        for record in invalid:
            record.valid = lambda: False

        self.assert_(not sets.valid(invalid))
        self.assert_(not sets.valid(self.records))

    def test_missing(self):
        """Test lazyboy.recordset.missing."""
        self.assert_(not sets.missing(self.records))
        records = rand_set(self.records)
        for record in records:
            record.missing = lambda: ('eggs', 'bacon')

        self.assert_(sets.missing(records))
        self.assert_(sets.missing(self.records))

    def test_modified(self):
        """Test lazyboy.recordset.modified."""
        self.assert_(not sets.modified(self.records))
        records = rand_set(self.records)
        for record in records:
            record.is_modified = lambda: True

        self.assert_(sets.modified(records))
        self.assert_(tuple(sets.modified(records)) == tuple(records))
        self.assert_(sets.modified(self.records))


class TestRecordSet(unittest.TestCase):
    """Unit tests for lazyboy.recordset.RecordSet."""

    def setUp(self):
        """Prepare the test object."""
        self.object = sets.RecordSet()

    def _get_records(self, count, **kwargs):
        """Return records for testing."""
        records = []
        for n in range(count):
            record = Record()

            if kwargs:
                record.key = Key(**kwargs)
            records.append(record)

        return records

    def test_transform(self):
        """Make sure RecordSet._transoform() works correctly."""
        self.assert_(self.object._transform([]) == {})
        records = self._get_records(5, keyspace="eggs", column_family="bacon")
        out = self.object._transform(records)
        self.assert_(len(out) == len(records))
        for record in records:
            self.assert_(record.key.key in out)
            self.assert_(out[record.key.key] is record)

        for key in out:
            self.assert_(key == out[key].key.key)

    def test_init(self):
        """Make sure the object can be constructed."""
        records = self._get_records(5, keyspace="eggs", column_family="bacon")
        rs = sets.RecordSet(records)

    def test_basic(self):
        """Make sure basic functionality works."""
        records = self._get_records(5, keyspace="eggs", column_family="bacon")
        rs = sets.RecordSet(records)
        self.assert_(len(rs) == len(records))
        self.assert_(rs.values() == records)
        self.assert_(set(rs.keys()) == set(record.key.key
                                           for record in records))

    def test_append(self):
        """Make sure RecordSet.append() works."""
        records = self._get_records(5, keyspace="eggs", column_family="bacon")
        for record in records:
            self.object.append(record)
            self.assert_(record.key.key in self.object)
            self.assert_(self.object[record.key.key] is record)

        self.assert_(self.object.values() == records)

    def test_save_failures(self):
        """Make sure RecordSet.save() aborts if there are invalid records."""

        records = self._get_records(5, keyspace="eggs", column_family="bacon")

        for record in records:
            record.is_modified = lambda: True
            record.valid = lambda: False
            self.object.append(record)

        self.assertRaises(ErrorMissingField, self.object.save)

    def test_save(self):
        """Make sure RecordSet.save() works."""

        class FakeRecord(object):

            class Key(object):
                pass

            def __init__(self):
                self.saved = False
                self.key = self.Key()
                self.key.key = str(uuid.uuid4())

            def save(self):
                self.saved = True
                return self

            def is_modified(self):
                return True

            def valid(self):
                return True


        records = [FakeRecord() for x in range(10)]
        map(self.object.append, records)
        self.object.save()
        for record in records:
            self.assert_(record.saved)
            self.assert_(self.object[record.key.key] is record)


class KeyRecordSetTest(unittest.TestCase):

    def setUp(self):
        self.object = sets.KeyRecordSet()

    def test_batch_load(self):
        records, keys = [], []
        for x in range(10):
            record = Record()
            record.key = Key('eggs', 'bacon')
            record['number'] = x
            record['square'] = x * x
            records.append(record)
            keys.append(record.key)

        backing = {}
        for record in records:
            backing[record.key.key] = [ColumnOrSuperColumn(col)
                                       for col in record._columns.values()]

        mock_client = MockClient([])
        mock_client.multiget_slice = \
            lambda ks, keys, parent, pred, clvl: backing
        sets.itr.get_pool = lambda ks: mock_client

        out_records = self.object._batch_load(Record, keys)
        for record in out_records:
            self.assert_(isinstance(record, Record))
            self.assert_(record.key in keys)
            orig = records[records.index(record)]
            self.assert_(orig['number'] == record['number'])
            self.assert_(orig['square'] == record['square'])

    def test_init(self):
        """Make sure KeyRecordSet.__init__ works as expected"""
        fake_key = partial(Key, "Eggs", "Bacon")
        keys = [fake_key(str(uuid.uuid1())) for x in range(10)]
        rs = sets.KeyRecordSet(keys, Record)


if __name__ == '__main__':
    unittest.main()
