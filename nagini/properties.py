# -*- coding: utf8 -*-
from nagini.utility import parse_list
import logging
import json
import os


logger = logging.getLogger('nagini.properties')


class Properties(dict):
    FLOAT_RE = r'^\d+\.\d*$'

    def load(self, filename=None):
        if filename is None:
            if 'JOB_PROP_FILE' in os.environ:
                filename = os.environ['JOB_PROP_FILE']
            else:
                logger.warning('No JOB_PROP_FILE in os.environ')
                return

        with open(filename) as fd:
            for line in fd:
                line = line.strip()
                if not line.startswith('#'):
                    name, value = line.split('=', 1)
                    self[name] = value.replace(r'\:', ':')

    def dump(self, filename=None):
        if filename is None:
            if 'JOB_OUTPUT_PROP_FILE' in os.environ:
                filename = os.environ['JOB_OUTPUT_PROP_FILE']
            else:
                logger.warning('No JOB_OUTPUT_PROP_FILE int os.environ')
                return

        with open(filename, 'w') as fd:
            json.dump(self, fd)

    def get_list(self, name, val_func='auto'):
        """Parse value with key=name and transform it to list.
        If val_vun

        :param str name:
        :param func val_func:
        :return:
        """
        return parse_list(self.get(name, ''), val_func)

    def update_not_override(self, other):
        """Like `update` but not override existing keys"""
        for k, v in other.iteritems():
            if k not in self:
                self[k] = v


props = Properties()
props.load()
