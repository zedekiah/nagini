# -*- coding: utf8 -*-


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
