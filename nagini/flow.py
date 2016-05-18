# -*- coding: utf8 -*-
from nagini.client import AzkabanClient
from nagini.properties import props
from os.path import join
import yaml
import abc


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
    def start(cls, properties=None, concurrent_option="skip"):
        """Start flow from another flow

        :param dict[str,str] properties:
        :param str concurrent_option: possible values ignore/skip,
        pipeline, queue
        :rtype:
        """
        # LOAD CONFIG
        with open(join(props["working.dir"], "config.yml")) as fd:
            config = yaml.load(fd)

        client = AzkabanClient(config["server"]["host"])
        client.login(config["server"]["username"],
                     config["server"]["password"])
        return client.execute_flow(config["project"], cls.name or cls.__name__,
                                   properties,
                                   concurrentOption=concurrent_option)


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
