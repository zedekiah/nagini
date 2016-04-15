# -*- coding: utf8 -*-
from job import BaseJob, UploadToMySqlJob, MySqlQueryJob
from flow import BaseFlow, EmbeddedFlow
from target import Target, LocalTarget
import logging


try:  # Python 2.7+
    from logging import NullHandler
except ImportError:
    class NullHandler(logging.Handler):
        def emit(self, record):
            pass

logging.getLogger(__name__).addHandler(NullHandler())