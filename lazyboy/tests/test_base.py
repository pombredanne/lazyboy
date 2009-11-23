# -*- coding: utf-8 -*-
#
# © 2009 Digg, Inc. All rights reserved.
# Author: Ian Eure <ian@digg.com>
#
"""Unit tests for Lazyboy's CassandraBase."""

import unittest
from lazyboy.base import CassandraBase
import lazyboy.connection
from lazyboy.key import Key
from lazyboy.exceptions import ErrorIncompleteKey


class CassandraBaseTest(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super(CassandraBaseTest, self).__init__(*args, **kwargs)
        self.class_ = CassandraBase

    def _get_object(self, *args, **kwargs):
        return self.class_(*args, **kwargs)

    def setUp(self):
        self.__get_pool = lazyboy.connection.get_pool
        lazyboy.connection.get_pool = lambda keyspace: "Test"
        self.object = self._get_object()

    def tearDown(self):
        lazyboy.connection.get_pool = self.__get_pool
        del self.object

    def test_init(self):
        self.assert_(self.object._clients == {})

    def test_get_cas(self):
        self.assertRaises(ErrorIncompleteKey, self.object._get_cas)

        # Get with explicit tale
        self.assert_(self.object._get_cas('foo') == "Test")

        # Implicit based on pk
        self.object.pk = Key(keyspace='eggs', column_family="bacon",
                             key='sausage')
        self.assert_(self.object._get_cas('foo') == "Test")


if __name__ == '__main__':
    unittest.main()
