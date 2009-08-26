# -*- coding: utf-8 -*-
#
# A new Python file
#
# © 2009 Digg, Inc. All rights reserved.
# Author: Ian Eure <ian@digg.com>
#

import unittest
from lazyboy.base import CassandraBase
import lazyboy.connection
from lazyboy.key import Key
from lazyboy.exceptions import ErrorUnknownKeyspace, ErrorIncompleteKey

class CassandraBaseTest(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(CassandraBaseTest, self).__init__(*args, **kwargs)
        self.class_ =  CassandraBase

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

    def test_gen_uuid(self):
        self.assert_(type(self.object._gen_uuid()) == str)
        self.assert_(self.object._gen_uuid() != self.object._gen_uuid(),
                     "Unique IDs aren't very unique.")

if __name__ == '__main__':
    unittest.main()
