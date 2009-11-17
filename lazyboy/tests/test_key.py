# -*- coding: utf-8 -*-
#
# Lazyboy: PrimaryKey unit tests
#
# © 2009 Digg, Inc. All rights reserved.
# Author: Ian Eure <ian@digg.com>
#

import unittest

from cassandra.ttypes import ColumnPath

from lazyboy.key import Key
from lazyboy.exceptions import ErrorIncompleteKey


class KeyTest(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super(KeyTest, self).__init__(*args, **kwargs)
        self.allowed = ({'keyspace': 'egg', 'column_family': 'sausage',
                         'key': 'bacon'},
                        {'keyspace': 'egg', 'column_family': 'sausage',
                         'key': 'bacon', 'super_column': 'tomato'})
        self.denied = ({'keyspace': 'egg', 'key': 'bacon'},
                       {'keyspace': 'egg', 'key': 'bacon',
                        'super_column': 'sausage'})

    def test_init(self):
        for args in self.allowed:
            pk = Key(**args)
            for k in args:
                self.assert_(getattr(pk, k) == args[k],
                             "Expected `%s', for key `%s', got `%s'" % \
                                 (args[k], k, getattr(pk, k)))

        for args in self.denied:
            self.assertRaises(ErrorIncompleteKey, Key, **args)

    def test_gen_uuid(self):
        key = Key(keyspace="eggs", column_family="bacon")
        self.assert_(type(key._gen_uuid()) == str)
        self.assert_(key._gen_uuid() != key._gen_uuid(),
                     "Unique IDs aren't very unique.")

    def test_super(self):
        self.assert_(not Key(keyspace='eggs', column_family='bacon',
                             key='sausage').is_super())
        self.assert_(Key(keyspace='eggs', column_family='bacon',
                             key='sausage', super_column='tomato').is_super())

    def test_str(self):
        x = Key(keyspace='eggs', key='spam', column_family='bacon').__str__()
        self.assert_(type(x) is str)

    def test_unicode(self):
        pk = Key(keyspace='eggs', key='spam', column_family='bacon')
        x = pk.__unicode__()
        self.assert_(type(x) is unicode)
        self.assert_(str(x) == str(pk))

    def test_repr(self):
        pk = Key(keyspace='eggs', key='spam', column_family='bacon')
        self.assert_(unicode(pk) == repr(pk))

    def test_get_path(self):
        base_key = dict(keyspace='eggs', key='spam', column_family='bacon')
        key = Key(**base_key)
        path = key.get_path()
        self.assert_(isinstance(path, ColumnPath))
        self.assert_(path.column_family == key.column_family)
        self.assert_(base_key['keyspace'] == key.keyspace)

        keey = key.clone()
        path2 = keey.get_path()
        self.assert_(path2 == path)

        path = key.get_path(column="foo")
        self.assert_(path.column == "foo")

    def test_clone(self):
        pk = Key(keyspace='eggs', key='spam', column_family='bacon')
        ppkk = pk.clone()
        self.assert_(isinstance(ppkk, Key))
        self.assert_(repr(pk) == repr(ppkk))
        for k in ('keyspace', 'key', 'column_family'):
            self.assert_(getattr(pk, k) == getattr(ppkk, k))

        # Changes override base keys, but don't change them.
        _pk = pk.clone(key='sausage')
        self.assert_(hasattr(_pk, 'keyspace'))
        self.assert_(hasattr(_pk, 'column_family'))
        self.assert_(hasattr(_pk, 'key'))

        self.assert_(_pk.key == 'sausage')
        self.assert_(pk.key == 'spam')
        _pk = pk.clone(super_column='tomato')
        self.assert_(_pk.super_column == 'tomato')
        self.assertRaises(AttributeError, _pk.__getattr__, 'sopdfj')
        self.assert_(hasattr(pk, 'key'))

        # Changes to the base propagate to cloned PKs.
        pk.keyspace = 'beans'
        self.assert_(_pk.keyspace == 'beans')

        __pk = _pk.clone()
        self.assert_(__pk.keyspace == 'beans')
        pk.keyspace = 'tomato'
        self.assert_(__pk.keyspace == 'tomato')


if __name__ == '__main__':
    unittest.main()
