# -*- coding: utf-8 -*-
#
# © 2009 Digg, Inc. All rights reserved.
# Author: Chris Goffinet <goffinet@digg.com>
# Author: Ian Eure <ian@digg.com>
#

"""Lazyboy: Connections."""
from __future__ import with_statement
import random
import os
import threading
import socket
import time

from cassandra import Cassandra
from thrift import Thrift
from thrift.transport import TTransport, TSocket
from thrift.protocol import TBinaryProtocol
import thrift

import lazyboy.exceptions as exc
from contextlib import contextmanager

_SERVERS = {}
_CLIENTS = {}

def _retry_default_callback(attempt, exc):
    """Retry an attempt five times, then give up."""
    return attempt < 5

def retry(callback=None):
    """Retry an operation."""

    callback = callback or _retry_default_callback
    assert callable(callback)

    def __closure__(func):

        def __inner__(*args, **kwargs):
            attempt = 0
            while True:
                try:
                    return func(*args, **kwargs)
                except Exception, exc:
                    if not callback(attempt, exc):
                        raise exc
                    attempt += 1
        return __inner__
    return __closure__

def add_pool(name, servers, timeout=None, recycle=None):
    """Add a connection."""
    _SERVERS[name] = dict(servers=servers, timeout=timeout, recycle=recycle)

def get_pool(name):
    """Return a client for the given pool name."""
    key = str(os.getpid()) + threading.currentThread().getName() + name
    if key in _CLIENTS:
        return _CLIENTS[key]

    try:
        _CLIENTS[key] = Client(**_SERVERS[name])
        return _CLIENTS[key]
    except Exception, e:
        raise exc.ErrorCassandraClientNotFound(
            "Pool `%s' is not defined." % name)


class Client(object):

    """A wrapper around the Cassandra client which load-balances."""

    def __init__(self, servers, timeout=None, recycle=None):
        """Initialize the client."""
        self._servers = servers
        self._recycle = recycle
        self._timeout = timeout
        self._clients = [s for s in [self._build_server(*server.split(":")) \
                                         for server in servers] if s]
        self._current_server = random.randint(0, len(self._clients))

    @retry()
    def get(self, *args, **kwargs):
        """
        Parameters:
        - keyspace
        - key
        - column_path
        - consistency_level
        """
        with self.get_client() as client:
            return client.get(*args, **kwargs)

    @retry()
    def get_slice(self, *args, **kwargs):
        """
        Parameters:
        - keyspace
        - key
        - column_parent
        - predicate
        - consistency_level
        """
        with self.get_client() as client:
            return client.get_slice(*args, **kwargs)

    @retry()
    def multiget(self, *args, **kwargs):
        """
        Parameters:
        - keyspace
        - keys
        - column_path
        - consistency_level
        """
        with self.get_client() as client:
            return client.multiget(*args, **kwargs)

    @retry()
    def multiget_slice(self, *args, **kwargs):
        """
        Parameters:
        - keyspace
        - keys
        - column_parent
        - predicate
        - consistency_level
        """
        with self.get_client() as client:
            return client.multiget_slice(*args, **kwargs)

    @retry()
    def get_count(self, *args, **kwargs):
        """
        Parameters:
        - keyspace
        - key
        - column_parent
        - consistency_level
        """
        with self.get_client() as client:
            return client.get_count(*args, **kwargs)

    @retry()
    def get_key_range(self, *args, **kwargs):
        """
        Parameters:
        - keyspace
        - column_family
        - start
        - finish
        - count
        - consistency_level
        """
        with self.get_client() as client:
            return client.get_key_range(*args, **kwargs)

    @retry()
    def remove(self, *args, **kwargs):
        """
        Parameters:
        - keyspace
        - key
        - column_path
        - timestamp
        - consistency_level
        """
        with self.get_client() as client:
            return client.remove(*args, **kwargs)

    @retry()
    def get_string_property(self, *args, **kwargs):
        """
        Parameters:
        - property
        """
        with self.get_client() as client:
            return client.get_string_property(*args, **kwargs)

    @retry()
    def get_string_list_property(self, *args, **kwargs):
        """
        Parameters:
        - property
        """
        with self.get_client() as client:
            return client.get_string_list_property(*args, **kwargs)

    @retry()
    def describe_keyspace(self, *args, **kwargs):
        """
        Parameters:
        - keyspace
        """
        with self.get_client() as client:
            return client.describe_keyspace(*args, **kwargs)

    @retry()
    def batch_insert(self, *args, **kwargs):
        """
        Parameters:
        - keyspace
        - key
        - cfmap
        - consistency_level
        """
        with self.get_client() as client:
            return client.batch_insert(*args, **kwargs)

    @retry()
    def insert(self, *args, **kwargs):
        """
        Parameters:
        - keyspace
        - key
        - column_path
        - value
        - timestamp
        - consistency_level
        """
        with self.get_client() as client:
            return client.insert(*args, **kwargs)

    def _build_server(self, host, port):
        """Return a client for the given host and port."""
        try:
            socket = TSocket.TSocket(host, int(port))
            if self._timeout:
                socket.setTimeout(self._timeout)
            transport = TTransport.TBufferedTransport(socket)
            protocol = TBinaryProtocol.TBinaryProtocolAccelerated(transport)
            client = Cassandra.Client(protocol)
            client.transport = transport
            return client
        except Exception:
            return None

    def _get_server(self):
        """Return the next server (round-robin) from the list."""
        if self._clients is None or len(self._clients) == 0:
            raise exc.ErrorCassandraNoServersConfigured

        next_server = self._current_server % len(self._clients)
        self._current_server += 1
        return self._clients[next_server]

    def list_servers(self):
        """Return all servers we know about."""
        return self._clients

    def _connect(self):
        """Connect to Cassandra if not connected."""

        client = self._get_server()

        if client.transport.isOpen() and self._recycle:
            if (client.connect_time + self._recycle) > time.time():
                return client
            else:
                client.transport.close()

        elif client.transport.isOpen():
            return client

        try:
            client.transport.open()
            client.connect_time = time.time()
        except thrift.transport.TTransport.TTransportException, e:
            client.transport.close()
            raise exc.ErrorThriftMessage(e.message)

        return client

    @contextmanager
    def get_client(self):
        """Yield a Cassandra client connection."""
        client = None
        try:
            client = self._connect()
            yield client
        except (socket.error, Thrift.TException), e:
            message = e.message or "Transport error, reconnect"
            if client:
                client.transport.close()
            raise exc.ErrorThriftMessage(message)
