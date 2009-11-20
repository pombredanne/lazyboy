# -*- coding: utf-8 -*-
#
# © 2009 Digg, Inc. All rights reserved.
# Author: Ian Eure <ian@digg.com>
#
"""Record unit tests."""

import time
import math
import uuid
import random
import types
import unittest

from cassandra.ttypes import Column, SuperColumn, ColumnOrSuperColumn, \
    ColumnParent

import lazyboy.record
from lazyboy.view import View
from lazyboy.connection import Client
from lazyboy.key import Key
import lazyboy.exceptions as exc

Record = lazyboy.record.Record
MirroredRecord = lazyboy.record.MirroredRecord

from test_base import CassandraBaseTest


_last_cols = []
_inserts = []


class MockClient(Client):

    """A mock Cassandra client which returns canned responses"""

    def get_slice(self, *args, **kwargs):
        [_last_cols.pop() for i in range(len(_last_cols))]
        cols = []
        for i in range(random.randrange(1, 15)):
            cols.append(ColumnOrSuperColumn(
                    column=Column(name=uuid.uuid4().hex,
                                  value=uuid.uuid4().hex,
                                  timestamp=time.time())))
        _last_cols.extend(cols)
        return cols

    def batch_insert(self, keyspace, key, cfmap, consistency_level):
        _inserts.append(cfmap)
        return True

    def remove(self, keyspace, key, column_path, timestamp, consistency_level):
        return


class RecordTest(CassandraBaseTest):

    class Record(Record):
        _keyspace = 'sausage'
        _column_family = 'bacon'
        _required = ('eggs',)

    def __init__(self, *args, **kwargs):
        super(RecordTest, self).__init__(*args, **kwargs)
        self.class_ = self.Record

    def test_init(self):
        self.object = self._get_object({'id': 'eggs', 'title': 'bacon'})
        self.assert_(self.object['id'] == 'eggs')
        self.assert_(self.object['title'] == 'bacon')

    def test_make_key(self):
        """Test make_key."""
        key = self.object.make_key("eggs")
        self.assert_(isinstance(key, Key))
        self.assert_(key.key == "eggs")
        self.assert_(key.keyspace == self.object._keyspace)
        self.assert_(key.column_family == self.object._column_family)

    def test_default_key(self):
        self.assertRaises(exc.ErrorMissingKey, self.object.default_key)

    def test_set_key(self):
        self.object._keyspace = "spam"
        self.object._column_family = "bacon"
        self.object.set_key("eggs", "tomato")
        self.assert_(hasattr(self.object, 'key'))
        self.assert_(isinstance(self.object.key, Key))
        self.assert_(self.object.key.keyspace == "spam")
        self.assert_(self.object.key.column_family == "bacon")
        self.assert_(self.object.key.key == "eggs")
        self.assert_(self.object.key.super_column == "tomato")

    def test_get_indexes(self):
        self.object._indexes = ({}, {})
        self.assert_(tuple(self.object.get_indexes()) == ({}, {}))

    def test_valid(self):
        self.assert_(not self.object.valid())
        self.object['eggs'] = 'sausage'
        self.assert_(self.object.valid())

    def test_missing(self):
        self.object._clean()
        self.assert_(self.object.missing() == self.object._required)
        self.object['eggs'] = 'sausage'
        self.assert_(self.object.missing() == ())

    def test_clean(self):
        data = {'id': 'eggs', 'title': 'bacon'}
        self.object = self._get_object(data)
        for k in data:
            self.assert_(k in self.object, "Key %s was not set?!" % (k,))
            self.assert_(self.object[k] == data[k])

        self.object._clean()
        for k in data:
            self.assert_(not k in self.object)

    def test_sanitize(self):
        self.assert_(isinstance(self.object.sanitize(u'ÜNICÖDE'), str))

    def test_repr(self):
        self.assert_(isinstance(repr(self.object), str))

    def test_update(self):
        data = {'id': 'eggs', 'title': 'bacon'}
        self.object.update(data)
        for k in data:
            self.assert_(self.object[k] == data[k])

        self.object._clean()
        self.object.update(data.items())
        for k in data:
            self.assert_(self.object[k] == data[k])

        self.object._clean()
        self.object.update(**data)
        for k in data:
            self.assert_(self.object[k] == data[k])

    def test_setitem_getitem(self):
        data = {'id': 'eggs', 'title': 'bacon'}
        self.assertRaises(exc.ErrorInvalidValue, self.object.__setitem__,
                          "eggs", None)
        for k in data:
            self.object[k] = data[k]
            self.assert_(self.object[k] == data[k],
                         "Data not set in Record")
            self.assert_(k in self.object._columns,
                         "Data not set in Record.columns")
            self.assert_(self.object._columns[k].__class__ is Column,
                         "Record._columns[%s] is %s, not Column" % \
                             (type(self.object._columns[k]), k))
            self.assert_(self.object._columns[k].value == data[k],
                         "Value mismatch in Column, got `%s', expected `%s'" \
                             % (self.object._columns[k].value, data[k]))
            now = self.object.timestamp()
            self.assert_(self.object._columns[k].timestamp <= now,
                         "Expected timestamp <= %s, got %s" \
                             % (now, self.object._columns[k].timestamp))

            self.assert_(k not in self.object._deleted,
                         "Key was marked as deleted.")
            self.assert_(k in self.object._modified,
                         "Key not in modified list")

            del self.object[k]
            self.object[k] = data[k]
            self.assert_(k not in self.object._deleted)

        # Make sure setting an identical original value doesn't change
        self.object._inject(
            self.object.key,
            {"eggs": Column(name="eggs", value="bacon", timestamp=0)})
        self.assert_(self.object["eggs"] == "bacon")
        self.assert_(self.object._original["eggs"].timestamp == 0)
        self.object["eggs"] = "bacon"
        self.assert_(self.object._original["eggs"].timestamp == 0)

    def test_delitem(self):
        data = {'id': 'eggs', 'title': 'bacon'}
        for k in data:
            self.object[k] = data[k]
            self.assert_(self.object[k] == data[k],
                         "Data not set in Record")
            self.assert_(k not in self.object._deleted,
                         "Key was marked as deleted.")
            self.assert_(k in self.object._modified,
                         "Key not in modified list")
            del self.object[k]
            self.assert_(k not in self.object, "Key was not deleted.")
            self.assert_(k in self.object._deleted,
                         "Key was not marked as deleted.")
            self.assert_(k not in self.object._modified,
                         "Deleted key in modified list")
            self.assert_(k not in self.object._columns,
                         "Column was not deleted.")

    def get_mock_cassandra(self, keyspace=None):
        """Return a mock cassandra instance"""
        mock = None
        if not mock:
            mock = MockClient(['localhost:1234'])
        return mock

    def test_load(self):
        test_data = (Column(name="eggs", value="1"),
                     Column(name="bacon", value="2"),
                     Column(name="spam", value="3"))

        real_slice = lazyboy.record.iterators.slice_iterator
        try:
            lazyboy.record.iterators.slice_iterator = lambda *args: test_data

            key = Key(keyspace='eggs', column_family='bacon', key='tomato')
            self.object.make_key = lambda *args, **kwargs: key
            self.object.load('tomato')
            self.assert_(self.object.key is key)

            key = Key(keyspace='eggs', column_family='bacon', key='tomato')
            self.object.load(key)

        finally:
            lazyboy.record.iterators.slice_iterator = real_slice

        self.assert_(self.object.key is key)
        cols = dict([[column.name, column] for column in test_data])
        self.assert_(self.object._original == cols)

        for col in cols.values():
            self.assert_(self.object[col.name] == col.value)
            self.assert_(self.object._columns[col.name] == col)

    def test_get_batch_args(self):
        columns = (Column(name="eggs", value="1"),
                   Column(name="bacon", value="2"),
                   Column(name="sausage", value="3"))
        column_names = [col.name for col in columns]

        data = {'eggs': "1", 'bacon': "2", 'sausage': "3"}
        key = Key(keyspace='eggs', column_family='bacon', key='tomato')

        args = self.object._get_batch_args(key, columns)

        # Make sure the key is correct
        self.assert_(args[0] is key.keyspace)
        self.assert_(args[1] is key.key)
        self.assert_(isinstance(args[2], dict))
        keys = args[2].keys()
        self.assert_(len(keys) == 1)
        self.assert_(keys[0] == key.column_family)
        self.assert_(not isinstance(args[2][key.column_family],
                                    types.GeneratorType))
        for val in args[2][key.column_family]:
            self.assert_(isinstance(val, ColumnOrSuperColumn))
            self.assert_(val.column in columns)
            self.assert_(val.super_column is None)

        key.super_column = "spam"
        args = self.object._get_batch_args(key, columns)
        self.assert_(args[0] is key.keyspace)
        self.assert_(args[1] is key.key)
        self.assert_(isinstance(args[2], dict))

        keys = args[2].keys()
        self.assert_(len(keys) == 1)
        self.assert_(keys[0] == key.column_family)
        self.assert_(not isinstance(args[2][key.column_family],
                                    types.GeneratorType))
        for val in args[2][key.column_family]:
            self.assert_(isinstance(val, ColumnOrSuperColumn))
            self.assert_(val.column is None)
            self.assert_(isinstance(val.super_column, SuperColumn))
            self.assert_(val.super_column.name is key.super_column)
            self.assert_(hasattr(val.super_column.columns, '__iter__'))
            for col in val.super_column.columns:
                self.assert_(col.name in data.keys())
                self.assert_(col.value == data[col.name])

    def test_remove(self):
        data = {'eggs': "1", 'bacon': "2", 'sausage': "3"}
        self.object.update(data)
        self.object.key = Key("eggs", "bacon", "tomato")
        self.object._get_cas = self.get_mock_cassandra
        self.assert_(self.object.remove() is self.object)
        self.assert_(not self.object.is_modified())
        for (key, val) in data.iteritems():
            self.assert_(key not in self.object)

    def test_save(self):
        self.assertRaises(exc.ErrorMissingField, self.object.save)
        data = {'eggs': "1", 'bacon': "2", 'sausage': "3"}
        self.object.update(data)

        self.assertRaises(exc.ErrorMissingKey, self.object.save)

        key = Key(keyspace='eggs', column_family='bacon', key='tomato')
        self.object.key = key
        self.object._get_cas = self.get_mock_cassandra
        del self.object['bacon']
        # FIXME – This doesn't really work, in the sense that
        # self.fail() is never called, but it still triggers an error
        # which makes the test fail, due to incorrect arity in the
        # arguments to the lambda.
        MockClient.remove = lambda self, a, b, c, d, e: None
        self.object.load = lambda self: None

        res = self.object.save()
        self.assert_(res == self.object,
                     "Self not returned from Record.save")
        cfmap = _inserts[-1]
        self.assert_(isinstance(cfmap, dict))

        self.assert_(self.object.key.column_family in cfmap,
                     "PK family %s not in cfmap" %
                     (self.object.key.column_family,))

        for corsc in cfmap[self.object.key.column_family]:
            self.assert_(corsc.__class__ == ColumnOrSuperColumn)
            self.assert_(corsc.column and not corsc.super_column)
            col = corsc.column

            self.assert_(col.name in data,
                         "Column %s wasn't set from update()" % \
                             (col.name))
            self.assert_(data[col.name] == col.value,
                         "Value of column %s is wrong, %s ≠ %s" % \
                             (col.name, data[col.name], col.value))
            self.assert_(col == self.object._columns[col.name],
                         "Column from cf._columns wasn't used in mutation_t")

            self.assert_(
                self.object._original['eggs']
                is not self.object._columns['eggs'],
                "Internal state corrupted on save.")

    def test_save_index(self):

        class FakeView(object):

            def __init__(self):
                self.records = []

            def append(self, record):
                self.records.append(record)

        views = [FakeView(), FakeView()]
        saves = []
        self.object.get_indexes = lambda: views
        self.object._save_internal = lambda *args: saves.append(True)

        data = {'eggs': "1", 'bacon': "2", 'sausage': "3"}
        self.object.update(data)
        self.object._keyspace = "cleese"
        self.object._column_family = "gilliam"
        self.object.set_key("blah")
        self.object.save()
        for view in views:
            self.assert_(self.object in view.records)

    def test_save_mirror(self):

        class FakeMirror(object):

            def __init__(self):
                self.records = []

            def mirror_key(self, parent_record):
                self.records.append(parent_record)
                return parent_record.key.clone(column_family="mirror")

        mirrors = [FakeMirror(), FakeMirror()]
        saves = []
        self.object.get_mirrors = lambda: mirrors
        self.object._save_internal = lambda *args: saves.append(True)

        data = {'eggs': "1", 'bacon': "2", 'sausage': "3"}
        self.object.update(data)
        self.object._keyspace = "cleese"
        self.object._column_family = "gilliam"
        self.object.set_key("blah")
        self.object.save()
        for mirror in mirrors:
            self.assert_(self.object in mirror.records)

    def test_save_mirror_failure(self):
        data = {'eggs': "1", 'bacon': "2", 'sausage': "3"}
        saves = []
        self.object._save_internal = lambda *args: saves.append(True)
        self.object._keyspace = "cleese"
        self.object._column_family = "gilliam"
        self.object.set_key("blah")

        class BrokenMirror(object):

            def mirror_key(self, parent_record):
                raise Exception("Testing")

        class BrokenView(object):

            def mirror_key(self, parent_record):
                raise Exception("Testing")

        class FakeView(object):

            def __init__(self):
                self.records = []

            def append(self, record):
                self.records.append(record)

        # Make sure a broken mirror doesn't break the record or views.
        mirrors = [BrokenMirror(), BrokenMirror()]
        self.object.get_mirrors = lambda: mirrors

        self.assert_(not self.object.is_modified())
        self.object.update(data)
        self.assert_(self.object.is_modified())

        views = [FakeView()]
        self.object.get_indexes = lambda: views
        self.assertRaises(Exception, self.object.save)
        for view in views:
            self.assert_(self.object in view.records)

        self.assertRaises(Exception, self.object.save)

        not self.assert_(not self.object.is_modified())

        self.object.update(data)
        views = [FakeView()]
        self.object.get_indexes = lambda: views
        self.assertRaises(Exception, self.object.save)
        self.assert_(not self.object.is_modified())
        for view in views:
            self.assert_(self.object in view.records)

        views = [BrokenView()]
        self.object.get_indexes = lambda: views
        self.object.update(data)
        self.assertRaises(Exception, self.object.save)
        self.assert_(not self.object.is_modified())

    def test_revert(self):
        data = {'id': 'eggs', 'title': 'bacon'}
        for k in data:
            self.object._original[k] = Column(name=k, value=data[k])

        self.object.revert()

        for k in data:
            self.assert_(self.object[k] == data[k])

    def test_is_modified(self):
        data = {'id': 'eggs', 'title': 'bacon'}

        self.assert_(not self.object.is_modified(),
                     "Untouched instance is marked modified.")

        self.object.update(data)
        self.assert_(self.object.is_modified(),
                     "Altered instance is not modified.")


class MirroredRecordTest(unittest.TestCase):

    """Tests for MirroredRecord"""

    def setUp(self):
        self.object = MirroredRecord()

    def test_mirror_key(self):
        self.assertRaises(exc.ErrorMissingKey, self.object.mirror_key,
                          self.object)

    def test_save(self):
        self.assertRaises(exc.ErrorImmutable, self.object.save)


if __name__ == '__main__':
    unittest.main()
