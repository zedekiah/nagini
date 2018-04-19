# -*- coding: utf8 -*-
from nagini.job import BaseJob, UploadToMySqlJob, MySqlQueryJob
from nagini.flow import BaseFlow, EmbeddedFlow
from nagini.target import Target, LocalTarget
from nagini.yaml_config import YamlConfig
from nagini.fields import *

from os.path import exists
import logging.config
import logging
import yaml


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
        config = yaml.load(fd)
        if 'logging' in config:
            logging.config.dictConfig(config['logging'])


logging.getLogger('nagini').addHandler(NullHandler())
