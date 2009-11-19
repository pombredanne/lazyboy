# -*- coding: utf-8 -*-
#
# © 2009 Digg, Inc. All rights reserved.
# Author: Ian Eure <ian@digg.com>
#

"""Iterator-based Cassandra tools."""

from itertools import groupby
from operator import attrgetter
from collections import defaultdict

from lazyboy.connection import get_pool
import lazyboy.exceptions as exc

from cassandra.ttypes import SlicePredicate, SliceRange, ConsistencyLevel, \
    ColumnOrSuperColumn, Column, ColumnParent


GET_KEYSPACE = attrgetter("keyspace")
GET_COLFAM = attrgetter("column_family")
GET_KEY = attrgetter("key")
GET_SUPERCOL = attrgetter("super_column")


def groupsort(iterable, keyfunc):
    """Return a generator which sort and groups a list."""
    return groupby(sorted(iterable, key=keyfunc), keyfunc)


def slice_iterator(key, consistency, **range_args):
    """Return an iterator over a row."""

    kwargs = {'start': "", 'finish': "",
              'count': 100000, 'reversed': 0}
    kwargs.update(range_args)

    consistency = consistency or ConsistencyLevel.ONE

    client = get_pool(key.keyspace)
    res = client.get_slice(
        key.keyspace, key.key, key,
        SlicePredicate(slice_range=SliceRange(**kwargs)),
        consistency)

    if not res:
        raise exc.ErrorNoSuchRecord("No record matching key %s" % key)

    return unpack(res)


def multigetterator(keys, consistency, **range_args):
    """Return a dictionary of data from Cassandra.

    This fetches data with the minumum number of network requests. It
    DOES NOT preserve order.

    If you depend on ordering, use list_multigetterator. This may
    require more requests.
    """
    kwargs = {'start': "", 'finish': "",
              'count': 100000, 'reversed': 0}
    kwargs.update(range_args)
    predicate = SlicePredicate(slice_range=SliceRange(**kwargs))
    consistency = consistency or ConsistencyLevel.ONE

    out = {}
    for (keyspace, ks_keys) in groupsort(keys, GET_KEYSPACE):
        client = get_pool(keyspace)
        out[keyspace] = {}
        for (colfam, cf_keys) in groupsort(ks_keys, GET_COLFAM):

            if colfam not in keyspace:
                out[keyspace][colfam] = defaultdict(lambda: dict())

            for (supercol, sc_keys) in groupsort(cf_keys, GET_SUPERCOL):
                records = client.multiget_slice(
                    keyspace, map(GET_KEY, sc_keys),
                    ColumnParent(colfam, supercol), predicate, consistency)

                for (row_key, cols) in records.iteritems():
                    cols = unpack(cols)
                    if supercol is None:
                        out[keyspace][colfam][row_key] = cols
                    else:
                        out[keyspace][colfam][row_key][supercol] = cols

    return out


def sparse_get(key, columns):
    """Return an iterator over a specific set of columns."""

    client = get_pool(key.keyspace)
    res = client.get_slice(
        key.keyspace, key.key, key, SlicePredicate(column_names=columns),
        ConsistencyLevel.ONE)

    return unpack(res)


def sparse_multiget(keys, columns):
    """Return an iterator over a specific set of columns."""

    first_key = iter(keys).next()
    client = get_pool(first_key.keyspace)
    row_keys = [key.key for key in keys]
    res = client.multiget_slice(
        first_key.keyspace, row_keys, first_key,
        SlicePredicate(column_names=columns), ConsistencyLevel.ONE)

    out = {}
    for (row_key, cols) in res.iteritems():
        out[row_key] = [corsc.column or corsc.super_column for corsc in cols]
    return out


def key_range(key, start="", finish="", count=100):
    """Return an iterator over a range of keys."""
    cas = get_pool(key.keyspace)
    return cas.get_key_range(key.keyspace, key.column_family, start,
                                  finish, count, ConsistencyLevel.ONE)


def key_range_iterator(key, start="", finish="", count=100):
    """Return an iterator which produces Key instances for a key range."""
    return (key.clone(key=k) for k in key_range(key, start, finish, count))


def pack(objects):
    """Return a generator which packs objects into ColumnOrSuperColumns."""
    for object_ in objects:
        key = 'column' if isinstance(object_, Column) else 'super_column'
        yield ColumnOrSuperColumn(**{key: object_})


def unpack(records):
    """Return a generator which unpacks objects from ColumnOrSuperColumns."""
    return (corsc.column or corsc.super_column for corsc in records)
