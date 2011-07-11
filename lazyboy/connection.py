# -*- coding: utf-8 -*-
#
# © 2009, 2010 Digg, Inc. All rights reserved.
# Author: Chris Goffinet <goffinet@digg.com>
# Author: Ian Eure <ian@digg.com>
#

"""Lazyboy: Connections."""
from __future__ import with_statement
from functools import update_wrapper
import logging
import random
import os
import threading
import socket
import errno
import time

from cassandra import Cassandra
import cassandra.ttypes as cas_types
from thrift import Thrift
from thrift.transport import TTransport, TSocket
from thrift.protocol import TBinaryProtocol
import thrift

import lazyboy.exceptions as exc
from contextlib import contextmanager

_SERVERS = {}
_CLIENTS = {}
RETRY_ATTEMPTS = 5

def _retry_default_callback(attempt, exc_):
    """Retry an attempt five times, then give up."""
    return attempt < RETRY_ATTEMPTS


def retry(callback=None):
    """Retry an operation."""

    callback = callback or _retry_default_callback
    assert callable(callback)

    def __closure__(func):

        def __inner__(*args, **kwargs):
            attempt = 1
            while True:
                try:
                    return func(*args, **kwargs)
                except Exception, ex:
                    if not callback(attempt, ex):
                        raise ex
                    attempt += 1
        return __inner__
    return __closure__


def add_pool(keyspace, servers, timeout=None, recycle=None, **kwargs):
    """Add a connection."""
    _SERVERS[keyspace] = dict(keyspace=keyspace, servers=servers, timeout=timeout, recycle=recycle,
                              **kwargs)


def get_pool(name):
    """Return a client for the given pool name."""
    key = str(os.getpid()) + threading.currentThread().getName() + name
    if key in _CLIENTS:
        return _CLIENTS[key]

    try:
        _CLIENTS[key] = Client(**_SERVERS[name])
        return _CLIENTS[key]
    except KeyError:
        raise exc.ErrorCassandraClientNotFound(
            "Pool `%s' is not defined." % name)


class _DebugTraceFactory(type):

    """A factory for making debug-tracing clients."""

    def __new__(mcs, name, bases, dct):
        """Create a new tracing client class."""
        assert len(bases) == 1, "Sorry, we don't do multiple inheritance."
        new_class = type(name, bases, dct)

        base_class = bases[0]
        for attrname in dir(base_class):
            attr = getattr(base_class, attrname)

            if (attrname.startswith('__') or attrname.startswith('send_')
                or attrname.startswith('recv_') or not callable(attr)):
                continue

            def wrap(func):
                """Returns a new wrapper for a function."""

                def __wrapper__(self, *args, **kwargs):
                    """A funcall wrapper."""
                    start_time = time.time()
                    try:
                        out = func(self, *args, **kwargs)
                    except Exception, ex:
                        self.log.error(
                            "Caught %s while calling: %s:%s -> %s(%s, %s)",
                            ex.__class__.__name__, self.host, self.port,
                            func.__name__, args, kwargs)
                        raise

                    end_time = time.time()

                    elapsed = (end_time - start_time) * 1000
                    log_func = (self.log.warn if elapsed >= self._slow_thresh
                                else self.log.debug)

                    log_func("%dms: %s:%s -> %s(%s, %s)",
                             elapsed, self.host, self.port,
                             func.__name__, args, kwargs)

                    return out

                update_wrapper(__wrapper__, func)
                return __wrapper__

            setattr(new_class, attrname, wrap(attr))

        return new_class


class DebugTraceClient(Cassandra.Client):

    """A client with debug tracing and slow query logging."""

    __metaclass__ = _DebugTraceFactory

    def __init__(self, *args, **kwargs):
        """Initialize"""
        slow_thresh = kwargs.get('slow_thresh', 100)
        log = kwargs.get('log', logging.getLogger(self.__class__.__name__))
        if 'slow_thresh' in kwargs:
            del kwargs['slow_thresh']
        if 'log' in kwargs:
            del kwargs['log']
        Cassandra.Client.__init__(self, *args, **kwargs)
        self._slow_thresh = slow_thresh
        self.log = log


class Client(object):

    """A wrapper around the Cassandra client which load-balances."""

    def __init__(self, keyspace, servers, timeout=None, recycle=None, debug=False,
                 **conn_args):
        """Initialize the client."""
        self._servers = servers
        self._recycle = recycle
        self._timeout = timeout
        self.keyspace = keyspace
        
        class_ = DebugTraceClient if debug else Cassandra.Client
        self._clients = [s for s in
                         [self._build_server(class_, *server.split(":"),
                                             **conn_args)
                          for server in servers] if s]
        self._current_server = random.randint(0, len(self._clients))
        
    def _build_server(self, class_, host, port, **conn_args):
        """Return a client for the given host and port."""
        try:
            socket_ = TSocket.TSocket(host, int(port))
            if self._timeout:
                socket_.setTimeout(self._timeout)
            transport = TTransport.TFramedTransport(socket_)
            protocol = TBinaryProtocol.TBinaryProtocolAccelerated(transport)
            client = class_(protocol, **conn_args)
            client.transport = transport
            setattr(client, 'host', host)
            setattr(client, 'port', port)
            return client
        except (Thrift.TException, cas_types.InvalidRequestException,
                cas_types.UnavailableException):
            return None

    def _get_server(self):
        """Return the next server (round-robin) from the list."""
        if self._clients is None or len(self._clients) == 0:
            raise exc.ErrorCassandraNoServersConfigured()

        self._current_server = self._current_server % len(self._clients)
        return self._clients[self._current_server]

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
            client.set_keyspace(self.keyspace)
        except thrift.transport.TTransport.TTransportException, ex:
            client.transport.close()
            raise exc.ErrorThriftMessage(
                ex.message, self._servers[self._current_server])

        
        return client

    @contextmanager
    def get_client(self):
        """Yield a Cassandra client connection."""
        client = None
        try:
            client = self._connect()
            yield client
        except socket.error, ex:
            if client:
                client.transport.close()

            args = (errno.errorcode[ex.args[0]], ex.args[1],
                    self._servers[self._current_server])
            raise exc.ErrorThriftMessage(*args)
        except Thrift.TException, ex:
            message = ex.message or "Transport error, reconnect"
            if client:
                client.transport.close()
            raise exc.ErrorThriftMessage(message,
                                         self._servers[self._current_server])
        except (cas_types.NotFoundException, cas_types.UnavailableException,
                cas_types.InvalidRequestException), ex:
            ex.args += (self._servers[self._current_server],
                        "on %s" % self._servers[self._current_server])
            raise ex

    @retry()
    def set_keyspace(self, *args, **kwargs):
        """
        Parameters:
        - keyspace
        """
        with self.get_client() as client:
            return client.set_keyspace(*args, **kwargs)
        
    @retry()
    def login(self, *args, **kwargs):
        """
        Parameters:
        - keyspace
        - auth_request
        """
        with self.get_client() as client:
            return client.login(*args, **kwargs)
        
    @retry()
    def get(self, *args, **kwargs):
        """
        Parameters:
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
        - key
        - column_parent
        - predicate
        - consistency_level
        """
        with self.get_client() as client:
            return client.get_slice(*args, **kwargs)

    @retry()
    def get_range_slice(self, *args, **kwargs):
        """
        returns a subset of columns for a range of keys.

        Parameters:
        - column_parent
        - predicate
        - start_key
        - finish_key
        - row_count
        - consistency_level
        """
        with self.get_client() as client:
            return client.get_range_slice(*args, **kwargs)

    @retry()
    def multiget(self, *args, **kwargs):
        """
        Parameters:        
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
        
        - key
        - cfmap
        - consistency_level
        """
        with self.get_client() as client:
            return client.batch_insert(*args, **kwargs)

    @retry()
    def batch_mutate(self, *args, **kwargs):
        """
        Parameters:
        
        - mutation_map
        - consistency_level
        """
        with self.get_client() as client:
            return client.batch_mutate(*args, **kwargs)

    @retry()
    def insert(self, *args, **kwargs):
        """
        Parameters:
        
        - key
        - column_path
        - value
        - timestamp
        - consistency_level
        """
        with self.get_client() as client:
            return client.insert(*args, **kwargs)
