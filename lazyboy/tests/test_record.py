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
import unittest

from cassandra.ttypes import Column, ColumnOrSuperColumn, ColumnParent

from lazyboy.connection import Client
from lazyboy.key import Key
from lazyboy.record import Record
from lazyboy.exceptions import ErrorMissingField, ErrorMissingKey, \
    ErrorInvalidValue

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


class RecordTest(CassandraBaseTest):
    class Record(Record):
        _key = {'table': 'eggs',
                'family': 'bacon'}
        _required = ('eggs',)

    def __init__(self, *args, **kwargs):
        super(RecordTest, self).__init__(*args, **kwargs)
        self.class_ = self.Record


    def test_init(self):
        self.object = self._get_object({'id': 'eggs', 'title': 'bacon'})
        self.assert_(self.object['id'] == 'eggs')
        self.assert_(self.object['title'] == 'bacon')

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
        self.assertRaises(ErrorInvalidValue, self.object.__setitem__,
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
            self.assert_(k in self.object._modified, "Key not in modified list")

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
        if not mock: mock = MockClient(['localhost:1234'])
        return mock

    def test_load(self):
        self.object._get_cas = self.get_mock_cassandra
        key = Key(keyspace='eggs', column_family='bacon', key='tomato')
        self.object.load(key)
        self.assert_(self.object.key is key)
        cols = dict([[obj.column.name, obj.column] for obj in _last_cols])
        self.assert_(self.object._original == cols)

        for col in cols.values():
            self.assert_(self.object[col.name] == col.value)
            self.assert_(self.object._columns[col.name] == col)

    def test_save(self):
        self.assertRaises(ErrorMissingField, self.object.save)
        data = {'eggs': "1", 'bacon': "2", 'sausage': "3"}
        self.object.update(data)

        self.assertRaises(ErrorMissingKey, self.object.save)

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


# class ImmutableRecordTest(RecordTest):
#     class ImmutableRecord(ImmutableRecord, RecordTest.class_):
#         _immutable = {'foo': 'xyz'}

#     def __init__(self, *args, **kwargs):
#         self.class_ = self.ImmutableRecord
#         super(RecordTest, self).__init__(*args, **kwargs)

#     def test_immutability(self):
#         try:
#             self.object['foo'] = 'bar'
#             self.fail("ErrorInvalidField not raised")
#         except ErrorInvalidField:
#             pass

#         try:
#             del self.object['foo']
#             self.fail("ErrorInvalidField not raised")
#         except ErrorInvalidField:
#             pass

#         self.assertRaises(ErrorInvalidField, self.object.update,
#                           {'foo': 'bar'})


if __name__ == '__main__':
    unittest.main()
