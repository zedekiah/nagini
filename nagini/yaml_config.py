# -*- coding: utf8 -*-
from os.path import expanduser, exists
import logging
import yaml


logger = logging.getLogger(__name__)


class YamlConfig(object):
    def __init__(self, filename='/etc/nagini.other.yaml'):
        self.filename = expanduser(filename)
        if exists(self.filename):
            with open(self.filename) as fd:
                self._data = yaml.load(fd)
        else:
            self._data = {}

    def get(self, name, default=None):
        path = name.split('/')
        root = self._data
        for i in path:
            if i in root:
                root = root[i]
            else:
                return default
        return root

    def set(self, name, value):
        path = name.split('/')
        root = self._data
        for i in path:
            if i == path[-1]:
                root[i] = value
            else:
                if i not in root:
                    root[i] = {}
                root = root[i]

    def save(self, filename=None):
        full_path = expanduser(filename or self.filename)
        with open(full_path, 'w') as fd:
            yaml.dump(self._data, fd, default_flow_style=False)

    def is_empty(self):
        return not bool(len(self._data))
