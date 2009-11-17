# -*- coding: utf-8 -*-
#
# © 2009 Digg, Inc. All rights reserved.
# Author: Ian Eure <ian@digg.com>
#

"""Unit tests for lazyboy.iterators."""

import unittest
import types
from pprint import pprint

from lazyboy.key import Key
import lazyboy.exceptions as exc
import lazyboy.iterators as iterators
import cassandra.ttypes as ttypes
from test_record import MockClient

from cassandra.ttypes import ConsistencyLevel, Column, SuperColumn


class SliceIteratorTest(unittest.TestCase):

    """Test suite for lazyboy.iterators.slice_iterator."""

    def setUp(self):
        """Prepare the test fixture."""
        self.__get_pool = iterators.get_pool
        self.client = MockClient(['127.0.0.1:9160'])
        iterators.get_pool = lambda pool: self.client

    def tearDown(self):
        """Tear down the test fixture."""
        iterators.get_pool = self.__get_pool

    def test_slice_iterator(self):
        """Test slice_iterator."""

        key = Key(keyspace="eggs", column_family="bacon", key="tomato")
        slice_iterator = iterators.slice_iterator(key, ConsistencyLevel.ONE)
        self.assert_(isinstance(slice_iterator, types.GeneratorType))
        for col in slice_iterator:
            self.assert_(isinstance(col, ttypes.Column))

    def test_slice_iterator_error(self):

        class Client(object):

            def get_slice(*args, **kwargs):
                return None

        key = Key(keyspace="eggs", column_family="bacon", key="tomato")
        real_get_pool = iterators.get_pool
        try:
            iterators.get_pool = lambda pool: Client()
            self.assertRaises(exc.ErrorNoSuchRecord,
                              iterators.slice_iterator,
                              key, ConsistencyLevel.ONE)
        finally:
            iterators.get_pool = real_get_pool

    def test_slice_iterator_supercolumns(self):
        """Test slice_iterator with supercolumns."""
        key = Key(keyspace="eggs", column_family="bacon", key="tomato",
                  super_column="spam")

        cols = [bla.column for bla in self.client.get_slice()]
        scol = ttypes.SuperColumn(name="spam", columns=cols)
        corsc = ttypes.ColumnOrSuperColumn(super_column=scol)

        self.client.get_slice = lambda *args: [corsc]

        slice_iterator = iterators.slice_iterator(key, ConsistencyLevel.ONE)
        self.assert_(isinstance(slice_iterator, types.GeneratorType))
        for col in slice_iterator:
            self.assert_(isinstance(col, ttypes.SuperColumn))

    def test_multigetterator(self):
        """Test multigetterator."""
        keys = [Key("eggs", "bacon", "cleese"),
                Key("eggs", "bacon", "gilliam"),
                Key("eggs", "spam", "jones"),
                Key("eggs", "spam", "idle"),
                Key("tomato", "sausage", "chapman"),
                Key("tomato", "sausage", "palin")]

        def pack(cols):
            return list(iterators.pack(cols))

        data = {'eggs': # Keyspace
                {'bacon': # Column Family
                 {'cleese': pack([Column(name="john", value="cleese")]),
                  'gilliam': pack([Column(name="terry", value="gilliam")])},
                 'spam': # Column Family
                     {'jones': pack([Column(name="terry", value="jones")]),
                      'idle': pack([Column(name="eric", value="idle")])}},
                'tomato': # Keyspace
                {'sausage': # Column Family
                 {'chapman':
                      pack([SuperColumn(name="chap_scol", columns=[])]),
                  'palin':
                      pack([SuperColumn(name="pal_scol", columns=[])])}}}

        def multiget_slice(keyspace, keys, column_parent, predicate,
                           consistencylevel):
            return data[keyspace][column_parent.column_family]
        self.client.multiget_slice = multiget_slice

        res = iterators.multigetterator(keys, ConsistencyLevel.ONE)
        self.assert_(isinstance(res, dict))
        for (keyspace, col_fams) in res.iteritems():
            self.assert_(keyspace in res)
            self.assert_(isinstance(col_fams, dict))
            for (col_fam, rows) in col_fams.iteritems():
                self.assert_(col_fam in data[keyspace])
                self.assert_(isinstance(rows, dict))
                for (row_key, columns) in rows.iteritems():
                    self.assert_(row_key in data[keyspace][col_fam])
                    for column in columns:
                        if keyspace == 'tomato':
                            self.assert_(isinstance(column, SuperColumn))
                        else:
                            self.assert_(isinstance(column, Column))

    def test_sparse_get(self):
        """Test sparse_get."""
        key = Key(keyspace="eggs", column_family="bacon", key="tomato")

        getter = iterators.sparse_get(key, ['eggs', 'bacon'])
        self.assert_(isinstance(getter, types.GeneratorType))
        for col in getter:
            self.assert_(isinstance(col, ttypes.Column))

    def test_sparse_multiget(self):
        """Test sparse_multiget."""
        key = Key(keyspace="eggs", column_family="bacon", key="tomato")

        row_keys = ['cleese', 'jones', 'gilliam', 'chapman', 'idle', 'palin']

        keys = (key.clone(key=row_key) for row_key in row_keys)


        cols = self.client.get_slice()
        res = dict((row_key, cols) for row_key in row_keys)

        self.client.multiget_slice = lambda *args: res
        getter = iterators.sparse_multiget(keys, ['eggs', 'bacon'])
        for (row_key, cols) in getter.iteritems():
            self.assert_(isinstance(row_key, str))
            self.assert_(row_key in row_keys)
            self.assert_(isinstance(cols, list))
            for col in cols:
                self.assert_(isinstance(col, ttypes.Column))

    def test_key_range(self):
        """Test key_range."""
        key = Key(keyspace="eggs", column_family="bacon", key="tomato")
        keys = ['spam', 'eggs', 'sausage', 'tomato', 'spam']
        self.client.get_key_range = lambda *args: keys
        self.assert_(iterators.key_range(key) == keys)

    def test_key_range_iterator(self):
        """Test key_range_iterator."""
        key = Key(keyspace="eggs", column_family="bacon", key="tomato")
        keys = ['spam', 'eggs', 'sausage', 'tomato', 'spam']

        real_key_range = iterators.key_range
        iterators.key_range = lambda *args: keys
        try:
            key_iter = iterators.key_range_iterator(key)
            self.assert_(isinstance(key_iter, types.GeneratorType))
            for key in key_iter:
                self.assert_(isinstance(key, Key))
        finally:
            iterators.key_range = real_key_range

    def test_pack(self):
        """Test pack."""
        columns = self.client.get_slice()
        packed = iterators.pack(columns)
        self.assert_(isinstance(packed, types.GeneratorType))
        for obj in packed:
            self.assert_(isinstance(obj, ttypes.ColumnOrSuperColumn))

    def test_unpack(self):
        """Test unpack."""
        columns = self.client.get_slice()
        unpacked = iterators.unpack(columns)
        self.assert_(isinstance(unpacked, types.GeneratorType))

        for obj in unpacked:
            self.assert_(isinstance(obj, ttypes.Column))


if __name__ == '__main__':
    unittest.main()
