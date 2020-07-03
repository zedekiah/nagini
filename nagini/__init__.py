# -*- coding: utf8 -*-
from __future__ import absolute_import, division, print_function, unicode_literals

import logging
import logging.config
from os.path import exists

import yaml

from nagini.contrib.mysql import MySqlQueryJob, UploadToMySqlJob
from nagini.fields import *
from nagini.flow import BaseFlow, EmbeddedFlow
from nagini.job import BaseJob
from nagini.target import LocalTarget, Target
from nagini.yaml_config import YamlConfig

__all__ = ['BaseJob', 'BaseFlow', 'EmbeddedFlow', 'Target', 'LocalTarget',
           'BaseField', 'StringField', 'RegexpField', 'DateField',
           'DateTimeField', 'StringMonthField', 'UnicodeField', 'IntField',
           'BooleanField', 'FloatField', 'ListField', 'JsonField',
           'YamlConfig']


try:  # Python 2.7+
    from logging import NullHandler
except ImportError:
    class NullHandler(logging.Handler):
        def emit(self, record):
            pass


if exists('/etc/nagini.yml'):
    with open('/etc/nagini.yml') as fd:
        config = yaml.safe_load(fd)
        if 'logging' in config:
            logging.config.dictConfig(config['logging'])


logging.getLogger('nagini').addHandler(NullHandler())
