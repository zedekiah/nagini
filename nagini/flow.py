# -*- coding: utf8 -*-
from __future__ import absolute_import, division, print_function, unicode_literals

import abc
from os.path import join

import yaml

from nagini.client import AzkabanClient
from nagini.properties import props
from nagini.utility import flatten


class BaseFlow(object):
    __metaclass__ = abc.ABCMeta
    name = None
    retries = 0
    retry_backoff = 0

    @abc.abstractmethod
    def requires(self):
        return []

    def run(self):
        """Override if needed"""

    def execute(self):
        self.run()

    @classmethod
    def start(cls, properties=None, concurrent_option='skip'):
        """Start flow from another flow

        :param dict[str,str] properties:
        :param str concurrent_option: possible values ignore/skip,
        pipeline, queue
        :rtype:
        """
        # LOAD CONFIG
        with open(join(props['working.dir'], 'config.yml')) as fd:
            config = yaml.load(fd)

        client = AzkabanClient(config['server']['host'])
        client.login(config['server']['username'],
                     config['server']['password'])
        return client.execute_flow(config['project'], cls.name or cls.__name__,
                                   properties,
                                   concurrentOption=concurrent_option)

    def get_start_jobs(self):
        return list(BaseFlow._get_start_jobs(self))

    @staticmethod
    def _get_start_jobs(root):
        requires = flatten(root.requires())
        if requires:
            for parent in requires:
                for job in BaseFlow._get_start_jobs(parent):
                    yield job
        else:
            yield root


class EmbeddedFlow(BaseFlow):
    def requires(self):
        return []

    @abc.abstractmethod
    def source_flow(self):
        """Override me!
        This method must return instance of source flow
        """
        pass
