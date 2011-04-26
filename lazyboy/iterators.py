# -*- coding: utf-8 -*-
#
# © 2009, 2010 Digg, Inc. All rights reserved.
# Author: Ian Eure <ian@digg.com>
#

"""Iterator-based Cassandra tools."""

import itertools as it
from operator import attrgetter, itemgetter
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
    return it.groupby(sorted(iterable, key=keyfunc), keyfunc)


def slice_iterator(key, consistency, **predicate_args):
    """Return an iterator over a row."""

    predicate = SlicePredicate()
    if 'columns' in predicate_args:
        predicate.column_names = predicate_args['columns']
    else:
        args = {'start': "", 'finish': "",
                  'count': 100000, 'reversed': False}
        args.update(predicate_args)
        predicate.slice_range=SliceRange(**args)

    consistency = consistency or ConsistencyLevel.ONE

    client = get_pool(key.keyspace)
    res = client.get_slice(
        key.key, key, predicate, consistency)

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
              'count': 100000, 'reversed': False}
    kwargs.update(range_args)
    predicate = SlicePredicate(slice_range=SliceRange(**kwargs))
    consistency = consistency or ConsistencyLevel.ONE

    out = {}
    for (keyspace, ks_keys) in groupsort(keys, GET_KEYSPACE):
        client = get_pool(keyspace)
        out[keyspace] = {}
        for (colfam, cf_keys) in groupsort(ks_keys, GET_COLFAM):

            if colfam not in keyspace:
                out[keyspace][colfam] = defaultdict(dict)

            for (supercol, sc_keys) in groupsort(cf_keys, GET_SUPERCOL):
                records = client.multiget_slice(
                    map(GET_KEY, sc_keys),
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
        key.key, key, SlicePredicate(column_names=columns),
        ConsistencyLevel.ONE)

    return unpack(res)


def sparse_multiget(keys, columns):
    """Return an iterator over a specific set of columns."""

    first_key = iter(keys).next()
    client = get_pool(first_key.keyspace)
    row_keys = [key.key for key in keys]
    res = client.multiget_slice(
        row_keys, first_key,
        SlicePredicate(column_names=columns), ConsistencyLevel.ONE)

    out = {}
    for (row_key, cols) in res.iteritems():
        out[row_key] = [corsc.column or corsc.super_column for corsc in cols]
    return out


def key_range(key, start="", finish="", count=100):
    """Return an iterator over a range of keys."""
    cas = get_pool(key.keyspace)
    return cas.get_key_range(key.column_family, start,
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


def repeat_seq(seq, num=1):
    """Return a seq of seqs with elements of seq repeated n times.


    repeat_seq([0, 1, 2, 3, 4], 2) -> [[0, 0], [1, 1], [2, 2], [3, 3], [4, 4]]
    """
    return (it.repeat(x, num) for x in seq)


def repeat(seq, num):
    """Retuan seq with each element repeated n times.

    repeat([0, 1, 2, 3, 4], 2) -> [0, 0, 1, 1, 2, 2, 3, 3, 4, 4]
    """
    return chain_iterable(repeat_seq(seq, num))


def chain_iterable(iterables):
    """Yield values from a seq of seqs."""
    # from_iterable(['ABC', 'DEF']) --> A B C D E F
    for seq in iterables:
        for element in seq:
            yield element


def chunk_seq(seq, size):
    """Return a sequence in chunks.

    First, we use repeat() to create an infinitely repeating sequence
    of numbers, repeated 3 times:
    (0, 0, 0, 1, 1, 1, 2, 2, 2, 3, 3, … n, n)

    Then we zip this with the elements of the input seq:
    ((0, a), (0, b), (0, c), (1, d), (1, e), (1, f), … (13, z))

    This is passed into groupby, where the zipped seq is grouped by
    the first item, producing (len(seq)/size) groups of elements:
    ((0, group-0), (1, group-1), … (n, group-n))

    This is then fed to imap, which extracts the group-n iterator and
    materializes it to:
    ((a, b, c), (d, e, f), (g, h, i), …)
    """
    return it.imap(lambda elt: tuple(it.imap(itemgetter(1), elt[1])),
                   it.groupby(it.izip(repeat(it.count(), size), seq),
                              itemgetter(0)))
