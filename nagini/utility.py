# -*- coding: utf8 -*-
from properties import load_properties
from collections import defaultdict
import logging
import os
import re


def flatten(obj):
    """Creates a flat list of all all items in structured output
    (dicts, lists, items)

    >>> flatten({'a': 'foo', 'b': 'bar'})
    ['foo', 'bar']
    >>> flatten(['foo', ['bar', 'troll']])
    ['foo', 'bar', 'troll']
    >>> flatten('foo')
    ['foo']
    >>> flatten(42)
    [42]
    """
    if obj is None:
        return []
    flat = []
    if isinstance(obj, dict):
        for key, result in obj.iteritems():
            flat += flatten(result)
        return flat
    if isinstance(obj, basestring):
        return [obj]

    try:
        # if iterable
        for result in obj:
            flat += flatten(result)
        return flat
    except TypeError:
        pass

    return [obj]


def require(module_path, fromlist):
    global_dict = globals()
    fromlist = flatten(fromlist)
    module = __import__(module_path, fromlist=fromlist)
    for item in fromlist:
        global_dict[item] = getattr(module, item)
    print "print global_dict"
    for key, value in global_dict.iteritems():
        print key, "=", value
    print "print globals()"
    for key, value in globals().iteritems():
        print key, "=", value


class JobLogHandler(logging.FileHandler):
    def __init__(self, filename, mode='a', encoding=None, delay=0):
        filename = filename.format(
            env=defaultdict(lambda: "unknown", os.environ),
            props=defaultdict(lambda: "unknown", load_properties())
        )
        try:
            os.makedirs(os.path.dirname(filename))
        except OSError as e:
            if not e.errno == 17:  # 17 - Directory already exists
                raise
        super(JobLogHandler, self).__init__(filename, mode, encoding, delay)
