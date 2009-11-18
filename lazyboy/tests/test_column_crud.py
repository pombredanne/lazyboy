# -*- coding: utf-8 -*-
#
# © 2009 Digg, Inc. All rights reserved.
# Author: Ian Eure <ian@digg.com>
#

"""Unit tests for Lazyboy CRUD."""

import unittest
import cassandra.ttypes as cas_types

from lazyboy import column_crud as crud
from lazyboy import Key


class CrudTest(unittest.TestCase):

    """Test suite for Lazyboy CRUD."""

    class FakeClient(object):

        def __init__(self):
            self._column = None
            self._inserts = []

        def _set_column(self, column):
            self._column = column

        def get(self, *args, **kwargs):
            return cas_types.ColumnOrSuperColumn(column=self._column)

        def insert(self, *args):
            self._inserts.append(args)

        def __getattr__(self, attr):

            def __inner__(self, *args, **kwargs):
                return None
            return __inner__

    def setUp(self):
        self.client = self.FakeClient()
        crud.get_pool = lambda pool: self.client

    def test_get_column(self):
        col = cas_types.Column("eggs", "bacon", 123)
        self.client._set_column(col)
        self.assert_(crud.get_column(Key("cleese", "palin", "chapman"),
                                     "eggs", None) is col)

    def test_set_column(self):
        key = Key("cleese", "palin", "chapman")
        args = (key, "eggs", "bacon", 456, None)
        crud.set(*args)
        self.assert_(len(self.client._inserts) == 1)

    def test_get(self):
        col = cas_types.Column("eggs", "bacon", 123)
        self.client._set_column(col)
        self.assert_(crud.get(Key("cleese", "palin", "chapman"),
                                     "eggs", None) == "bacon")

    def test_set(self):
        key = Key("cleese", "palin", "chapman")
        args = (key, cas_types.Column("eggs", "bacon", 456), None)
        crud.set_column(*args)
        self.assert_(len(self.client._inserts) == 1)

    def test_remove(self):
        key = Key("cleese", "palin", "chapman")
        args = (key, "eggs", 456, None)
        crud.remove(*args)


if __name__ == '__main__':
    unittest.main()
