# -*- coding: utf8 -*-
from nagini.properties import load_properties, save_properties
import abc


class BaseFlow(object):
    __metaclass__ = abc.ABCMeta
    name = None
    retries = 0
    retry_backoff = 0

    def __init__(self):
        self.props = load_properties()

    @abc.abstractmethod
    def requires(self):
        return []

    def run(self):
        """Override if needed"""

    def execute(self):
        self.run()
        save_properties(self.props)


class EmbeddedFlow(BaseFlow):
    __metaclass__ = abc.ABCMeta

    def requires(self):
        return []

    @abc.abstractmethod
    def source_flow(self):
        """Override me!
        This method must return instance of source flow
        """
        pass
