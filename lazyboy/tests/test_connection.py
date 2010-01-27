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
import logging
import socket
from contextlib import contextmanager

from cassandra import Cassandra
from cassandra.ttypes import *
from thrift.transport.TTransport import TTransportException
from thrift import Thrift
from thrift.transport import TSocket

import lazyboy.connection as conn
from lazyboy.exceptions import *
from test_record import MockClient
from lazyboy.util import save, raises


class Generic(object):
    pass


class _MockTransport(object):

    def __init__(self, *args, **kwargs):
        self.calls = {'open': 0, 'close': 0}

    def open(self):
        self.calls['open'] += 1

    def close(self):
        self.calls['close'] += 1


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

        cls = Cassandra.Client
        srv = self.client._build_server(cls, 'localhost', 1234)
        self.assert_(isinstance(srv, Cassandra.Client))

        self.client._timeout = 250
        srv = self.client._build_server(cls, 'localhost', 1234)
        self.assert_(srv._iprot.trans._TBufferedTransport__trans._timeout ==
                     self.client._timeout * .001)
        self.assert_(isinstance(srv, Cassandra.Client))

        with save(conn.TSocket, ('TSocket',)):
            for exc_class in exc_classes:
                conn.TSocket.TSocket = raises(exc_class)
                self.assert_(self.client._build_server(cls, 'localhost', 1234)
                             is None)

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
        client = self.client._get_server()
        self.client._get_server = lambda: client

        # Already connected
        client.transport = _MockTransport()
        client.transport.isOpen = lambda: True
        self.assert_(self.client._connect())

        # Not connected, no error
        nopens = client.transport.calls['open']
        with save(client.transport, ('isOpen',)):
            client.transport.isOpen = lambda: False
            self.assert_(self.client._connect())
            self.assert_(client.transport.calls['open'] == nopens + 1)

        # Thrift Exception on connect - trapped
        ncloses = client.transport.calls['close']
        with save(client.transport, ('isOpen', 'open')):
            client.transport.isOpen = lambda: False
            client.transport.open = raises(TTransportException)
            self.assertRaises(ErrorThriftMessage, self.client._connect)
            self.assert_(client.transport.calls['close'] == ncloses +1)

        # Other exception on connect should be ignored
        with save(client.transport, ('isOpen', 'open')):
            client.transport.isOpen = lambda: False
            client.transport.open = raises(Exception)
            ncloses = client.transport.calls['close']
            self.assertRaises(Exception, self.client._connect)

        # Connection recycling - reuse same connection
        nopens = client.transport.calls['open']
        ncloses = client.transport.calls['close']
        conn = self.client._connect()
        self.client._recycle = 60
        client.connect_time = time.time()
        self.assert_(conn is self.client._connect())
        self.assert_(client.transport.calls['open'] == nopens)
        self.assert_(client.transport.calls['open'] == ncloses)

        # Recycling - reconnect
        nopens = client.transport.calls['open']
        ncloses = client.transport.calls['close']
        conn = self.client._connect()
        self.client._recycle = 60
        client.connect_time = 0
        self.assert_(conn is self.client._connect())
        self.assert_(client.transport.calls['open'] == nopens + 1)
        self.assert_(client.transport.calls['close'] == ncloses + 1)


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
        self.client._servers = [raw_server]
        self.client._current_server = 0

        transport = _MockTransport()
        raw_server.transport = transport

        with self.client.get_client() as clt:
            self.assert_(clt is raw_server)

        # Socket error handling
        ncloses = transport.calls['close']
        try:
            with self.client.get_client() as clt:
                raise socket.error(7, "Test error")
            self.fail_("Exception not raised.")
        except ErrorThriftMessage, exc:
            self.assert_(transport.calls['close'] == ncloses + 1)
            self.assert_(exc.args[1] == "Test error")

        closed = []
        try:
            raw_server.transport = Generic()
            raw_server.transport.close = lambda: closed.append(True)
            with self.client.get_client() as clt:
                raise Thrift.TException("Cleese")
        except ErrorThriftMessage, exc:
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

        # Cassandra exceptions - no open/close, added info
        excs = ((NotFoundException, UnavailableException,
                 InvalidRequestException))
        for exc_ in excs:
            try:
                with self.client.get_client() as clt:
                    raise exc_("John Cleese")
                self.fail_("Exception gobbled.")
            except (exc_), ex:
                self.assert_(len(ex.args) > 1)
                server = self.client._servers[self.client._current_server]
                self.assert_(repr(server) in ex.args[-1])


class TestRetry(unittest.TestCase):

    """Test retry logic."""

    def test_retry_default_callback(self):
        """Make sure retry_default_callback works."""
        for x in range(conn.RETRY_ATTEMPTS):
            self.assert_(conn._retry_default_callback(x, None))

        self.assert_(not conn._retry_default_callback(x + 1, None))

    def test_retry(self):
        """Test retry."""
        retries = []
        def bad_func():
            retries.append(True)
            raise Exception("Whoops.")

        retry_func = conn.retry()(bad_func)
        self.assertRaises(Exception, retry_func)
        self.assert_(len(retries) == conn.RETRY_ATTEMPTS)


class DebugTraceClientTest(unittest.TestCase):

    """Test the DebugTraceClient."""

    def test_init(self):
        with save(conn.DebugTraceClient, ('__metaclass__',)):
            del conn.DebugTraceClient.__metaclass__
            logger = logging.getLogger("TestCase")
            client = conn.DebugTraceClient(None, slow_thresh=150,
                                           log=logger)
            self.assert_(isinstance(client, Cassandra.Client))
            self.assert_(hasattr(client, 'log'))
            self.assert_(isinstance(client.log, logging.Logger))
            self.assert_(client.log is logger)
            self.assert_(hasattr(client, '_slow_thresh'))
            self.assert_(client._slow_thresh == 150)


class DebugTraceFactoryTest(unittest.TestCase):

    """Test DebugTraceFactory."""

    def setUp(self):
        Cassandra.Iface.raises = raises(Exception)

    def tearDown(self):
        del Cassandra.Iface.raises

    def test_multiple_inherit_exception(self):
        """Make sure we get an exception in multi-inherit cases."""
        TypeA = type('TypeA', (), {})
        TypeB = type('TypeB', (), {})

        mcs = conn._DebugTraceFactory
        self.assertRaises(AssertionError, mcs.__new__,
                          mcs, 'TypeC', (TypeA, TypeB), {})

    def test_trace_factory(self):
        """Make sure the trace factory works as advertised."""

        class Tracer(Cassandra.Iface):
            """Dummy class for testing the tracer."""

            __metaclass__ = conn._DebugTraceFactory


        for name in dir(Tracer):
            if name.startswith('__'):
                continue

            self.assert_(getattr(Tracer, name) != getattr(Cassandra.Iface, name),
                         "Child class shares attr %s" % name)


        error, warn, debug = [], [], []

        fake_log = type('FakeLog', (logging.Logger, object),
                        {'__init__': lambda self: None,
                         'warn': lambda self, *args: warn.append(args),
                         'debug': lambda self, *args: debug.append(args),
                         'error': lambda self, *args: error.append(args)})

        tr = Tracer()
        tr._slow_thresh = 0
        tr.log = fake_log()
        tr.host, tr.port = '127.0.0.1', 1337

        tr.get_string_property("Foo")
        self.assert_(len(warn) == 1)

        tr._slow_thresh = 100
        tr.get_string_property("Foo")
        self.assert_(len(warn) == 1)
        self.assert_(len(debug) == 1)

        self.assertRaises(Exception, tr.raises)
        self.assert_(len(error) == 1)


if __name__ == '__main__':
    unittest.main()
