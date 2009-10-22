# -*- coding: utf-8 -*-
#
# Connection unit tests
#
# © 2009 Digg, Inc. All rights reserved.
# Author: Ian Eure <ian@digg.com>
# Author: Chris Goffinet <goffinet@digg.com>
#

import unittest
import time

from cassandra import Cassandra
from cassandra.ttypes import *
from thrift import Thrift
from thrift.transport import TSocket

import lazyboy.connection as conn
from lazyboy.exceptions import *
from test_record import MockClient


class Generic(object):
    pass


class ConnectionTest(unittest.TestCase):

    def setUp(self):
        self.pool = 'testing'
        self.__client = conn.Client
        conn.Client = MockClient
        conn._CLIENTS = {}
        conn._SERVERS = {self.pool: ['localhost:1234']}

    def tearDown(self):
        conn.Client = self.__client


class TestPools(ConnectionTest):

    def test_add_pool(self):
        servers = ['localhost:1234', 'localhost:5678']
        conn.add_pool(__name__, servers)
        self.assert_(conn._SERVERS[__name__] == servers)

    def test_get_pool(self):
        client = conn.get_pool(self.pool)
        self.assert_(type(client) is conn.Client)

        self.assertRaises(TypeError, conn.Client)

        self.assertRaises(ErrorCassandraClientNotFound,
                          conn.get_pool, (__name__))


class TestClient(ConnectionTest):

    def setUp(self):
        super(TestClient, self).setUp()
        self.client = MockClient(['localhost:1234', 'localhost:5678'])

    def test_init(self):
        pass

    def test_build_server(self):

        class ErrorFailedBuild(Exception):
            pass

        def raise_():
            raise ErrorFailedBuild()


        srv = self.client._build_server('localhost', 1234)
        self.assert_(srv.__class__ is Cassandra.Client)

        # FIXME - test exception handling

    def test_get_server(self):
        # Zero clients
        real = self.client._clients
        bad = (None, [])
        for clts in bad:
            self.client._clients = bad
            self.assert_(ErrorCassandraNoServersConfigured,
                         self.client._get_server)

        # Round-robin
        fake = ['eggs', 'bacon', 'spam']
        self.client._clients = fake
        self.client._current_server = 0
        for exp in fake * 2:
            srv = self.client._get_server()
            self.assert_(srv == exp)

    def test_list_servers(self):
        servers = self.client.list_servers()
        self.assert_(servers.__class__ == list)
        self.assert_(self.client._clients == servers)

    def test_connect(self):

        def raise_(except_):

            def __r():
                raise except_
            return __r

        class _MockTransport(object):

            def __init__(self, *args, **kwargs):
                self.calls = {'open': 0, 'close': 0}

            def open(self):
                self.calls['open'] += 1

            def close(self):
                self.calls['close'] += 1

        # Already connected
        client = self.client._clients[0]
        client.transport = _MockTransport()
        client.transport.isOpen = lambda: True
        self.assert_(self.client._connect(client))

        # Not connected, no error
        client.transport.isOpen = lambda: False
        nopens = client.transport.calls['open']
        self.assert_(self.client._connect(client))
        self.assert_(client.transport.calls['open'] == nopens + 1)

        # Thrift Exception on connect
        client.transport.open = raise_(Thrift.TException)
        self.assertRaises(ErrorThriftMessage,
                          self.client._connect, client,)

        # Other exception on connect
        client.transport.open = raise_(Exception)
        ncloses = client.transport.calls['close']
        self.assert_(self.client._connect(client) == False)
        self.assert_(client.transport.calls['close'] == ncloses + 1)

    def test_getattr(self):
        getter = self.client.__getattr__('get_slice')
        self.assert_(callable(getter))

        self.client._get_server = lambda: None
        self.client._connect = lambda server: False
        getter = self.client.__getattr__('get_slice')
        self.assert_(getter() is None)

        client = Generic()
        client.get_slice = lambda *args, **kwargs: "slice"
        client.transport = Generic()
        client.transport.close = lambda: None
        self.client._connect = lambda server: True
        self.client._get_server = lambda: client
        self.assert_(getter() is "slice")

        def raises(exception_class):

            def raise_(*args, **kwargs):
                raise exception_class(*args, **kwargs)
            return raise_

        client.get_slice = lambda *args, **kwargs: raises(Exception)()
        getter = self.client.__getattr__('get_slice')
        self.assertRaises(Exception, getter)

        client.get_slice = lambda *args, **kwargs: raises(Thrift.TException)()
        getter = self.client.__getattr__('get_slice')
        self.assertRaises(ErrorThriftMessage, getter)

        client.get_slice = lambda *args, **kwargs: \
            raises(Thrift.TException)(message="Test 123")
        getter = self.client.__getattr__('get_slice')
        self.assertRaises(ErrorThriftMessage, getter)


if __name__ == '__main__':
    unittest.main()
