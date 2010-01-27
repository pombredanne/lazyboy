# -*- coding: utf-8 -*-
#
# © 2009, 2010 Digg, Inc. All rights reserved.
# Author: Ian Eure <ian@digg.com>
#

"""Utility functions."""

from __future__ import with_statement
from contextlib import contextmanager
import logging


def raise_(exc=None, *args, **kwargs):
    """Raise an exception."""
    raise (exc or Exception)(*args, **kwargs)


def raises(exc=None, *args, **kwargs):
    """Return a function which raises an exception when called."""

    def __inner__(*func_args, **func_kwargs):
        """Raise the exception."""
        raise_(exc, *args, **kwargs)
    return __inner__


@contextmanager
def save(obj, attrs=None):
    """Save attributes of an object, then restore them."""
    orig_attrs = {}
    for attr in attrs:
        orig_attrs[attr] = getattr(obj, attr)

    try:
        yield
    finally:
        for attr in attrs:
            try:
                setattr(obj, attr, orig_attrs[attr])
            except Exception:
                pass


def returns(value):
    """Return a function which returns a value, ignoring arguments."""

    def __return_inner(*args, **kwargs):
        """Return value."""
        return value

    return __return_inner


@contextmanager
def suppress(*args):
    """Run code while suppressing exceptions in args."""
    try:
        yield
    except args, exc:
        logging.warn("Suppressing: %s %s", type(exc), exc)
