# -*- coding: utf-8 -*-
#
# © 2009, 2010 Digg, Inc. All rights reserved.
# Author: Ian Eure <ian@digg.com>
# Author: Chris Goffinet <goffinet@digg.com>
#
"""Connection unit tests."""

from __future__ import with_statement
import unittest
import time
import types
from contextlib import contextmanager

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
        self._client = conn.Client
        conn.Client = MockClient
        conn._CLIENTS = {}
        conn._SERVERS = {self.pool: dict(servers=['localhost:1234'])}

    def tearDown(self):
        conn.Client = self._client


class TestPools(ConnectionTest):

    def test_add_pool(self):
        servers = ['localhost:1234', 'localhost:5678']
        conn.add_pool(__name__, servers)
        self.assert_(conn._SERVERS[__name__]["servers"] == servers)

    def test_get_pool(self):
        client = conn.get_pool(self.pool)
        self.assert_(type(client) is conn.Client)

        # Again, to cover the short-circuit conditional
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

        exc_classes = (InvalidRequestException, UnavailableException,
                       Thrift.TException)

        def raise_(exception):
            def __inner__(*args, **kwargs):
                raise exception()
            return __inner__

        srv = self.client._build_server('localhost', 1234)
        self.assert_(srv.__class__ is Cassandra.Client)

        _tsocket = conn.TSocket.TSocket
        try:
            for exc_class in exc_classes:
                conn.TSocket.TSocket = raise_(exc_class)
                self.assert_(self.client._build_server('localhost', 1234)
                             is None)
        finally:
            conn.TSocket.TSocket = _tsocket

    def test_get_server(self):
        # Zero clients
        real = self.client._clients
        bad = (None, [])
        for clts in bad:
            self.client._clients = clts
            self.assertRaises(ErrorCassandraNoServersConfigured,
                         self.client._get_server)

        # Round-robin
        fake = ['eggs', 'bacon', 'spam']
        self.client._clients = fake
        self.client._current_server = 0
        for exp in range(2 * len(fake)):
            srv = self.client._get_server()
            self.assert_(srv in fake)

    def test_list_servers(self):
        servers = self.client.list_servers()
        self.assert_(servers.__class__ == list)
        self.assert_(self.client._clients == servers)

    def test_connect(self):

        def raise_(except_, *args, **kwargs):

            def __r():
                raise except_(*args, **kwargs)
            return __r

        class _MockTransport(object):

            def __init__(self, *args, **kwargs):
                self.calls = {'open': 0, 'close': 0}

            def open(self):
                self.calls['open'] += 1

            def close(self):
                self.calls['close'] += 1

        # # Already connected
        # client = self.client._clients[0]
        # client.transport = _MockTransport()
        # client.transport.isOpen = lambda: True
        # self.assert_(self.client._connect())
        #
        # # Not connected, no error
        # client.transport.isOpen = lambda: False
        # nopens = client.transport.calls['open']
        # self.assert_(self.client._connect())
        # self.assert_(client.transport.calls['open'] == nopens + 1)

        # Thrift Exception on connect (no message)
        # client.transport.open = raise_(Thrift.TException)
        # self.assertRaises(ErrorThriftMessage,
        #                   self.client._connect)
        #
        # # Thrift Exception on connect (with message)
        # client.transport.open = raise_(Thrift.TException, "Cleese")
        # self.assertRaises(ErrorThriftMessage,
        #                   self.client._connect)
        #
        # # Other exception on connect
        # client.transport.open = raise_(Exception)
        # ncloses = client.transport.calls['close']
        # self.assert_(self.client._connect() == False)
        # self.assert_(client.transport.calls['close'] == ncloses + 1)

    def test_methods(self):
        """Test the various client methods."""

        methods = filter(lambda m: m[0] != '_', dir(Cassandra.Iface))

        real_client = Generic()

        @contextmanager
        def get_client():
            yield real_client

        client = self._client(['127.0.0.1:9160'])
        client.get_client = get_client
        dummy = lambda *args, **kwargs: (True, args, kwargs)

        for method in methods:
            self.assert_(hasattr(client, method),
                         "Lazyboy client lacks interface method %s" % method)
            self.assert_(callable(getattr(client, method)))
            setattr(real_client, method, dummy)
            res = getattr(client, method)('cleese', gilliam="Terry")
            self.assert_(isinstance(res, tuple),
                         "%s method failed: %s" % (method, res))
            self.assert_(res[0] is True)
            self.assert_(res[1] == ('cleese',))
            self.assert_(res[2] == {'gilliam': "Terry"})

    def test_get_client(self):
        """Test get_client."""
        cass_client = Generic()
        raw_server = Generic()
        self.client._get_server = lambda: raw_server
        self.client._connect = lambda: raw_server

        with self.client.get_client() as clt:
            self.assert_(clt is raw_server)

        closed = []
        try:
            raw_server.transport = Generic()
            raw_server.transport.close = lambda: closed.append(True)
            with self.client.get_client() as clt:
                raise Thrift.TException("Cleese")
        except Exception, exc:
            self.assert_(len(closed) == 1)
            self.assert_(exc.args[0] == "Cleese")

        closed = []
        try:
            raw_server.transport = Generic()
            raw_server.transport.close = lambda: closed.append(True)
            with self.client.get_client() as clt:
                raise Thrift.TException()
        except Exception, exc:
            self.assert_(len(closed) == 1)
            self.assert_(exc.args[0] != "")


if __name__ == '__main__':
    unittest.main()
