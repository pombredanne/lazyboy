# -*- coding: utf-8 -*-
#
# © 2009 Digg, Inc. All rights reserved.
# Author: Ian Eure <ian@digg.com>
#

"""Column-level operations."""

from cassandra import ttypes as cas_types

from lazyboy.connection import get_pool
from lazyboy.iterators import unpack


def get_column(key, column_name, consistency=None):
    """Get a column."""
    consistency = consistency or cas_types.ConsistencyLevel.ONE
    return unpack(
        [get_pool(key.keyspace).get(
            key.keyspace, key.key,
            key.get_path(column=column_name), consistency)]).next()


def set_column(key, column, consistency=None):
    """Set a column."""
    assert isinstance(column, cas_types.Column)
    consistency = consistency or cas_types.ConsistencyLevel.ONE
    return set(key, column.name, column.value, column.timestamp, consistency)


def get(key, column, consistency=None):
    """Get the value of a column."""
    return get_column(key, column, consistency).value


def set(key, name, value, timestamp=None, consistency=None):
    """Set a column's value."""
    consistency = consistency or cas_types.ConsistencyLevel.ONE
    get_pool(key.keyspace).insert(
        key.keyspace, key.key, key.get_path(column=name), value, timestamp,
        consistency)


def remove(key, column, timestamp=None, consistency=None):
    """Remove a column."""
    consistency = consistency or cas_types.ConsistencyLevel.ONE
    get_pool(key.keyspace).remove(key.keyspace, key.key,
                                  key.get_path(column=column), timestamp,
                                  consistency)
