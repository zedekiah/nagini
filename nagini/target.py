# -*- coding: utf8 -*-
import abc
import os


class Target(object):
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def exists(self):
        return False

    @abc.abstractmethod
    def clean_up(self):
        pass


class LocalTarget(Target):
    def __init__(self, path):
        self.path = path

    def exists(self):
        return os.path.exists(self.path)

    def open(self, mode="r"):
        if mode == "w":
            # Create folder if it does not exist
            normpath = os.path.normpath(self.path)
            parentfolder = os.path.dirname(normpath)
            if parentfolder and not os.path.exists(parentfolder):
                os.makedirs(parentfolder)

        return open(self.path, mode)

    def clean_up(self):
        if os.path.exists(self.path):
            os.remove(self.path)


class MySqlTarget(Target):
    def __init__(self, table, db=None, host=None, port=None, user=None,
                 password=None, config_file=None, clear=False, fields=None):
        self.table = table
        self.db = db
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.config_file = config_file
        self.clear = clear
        self.fields = fields

    def exists(self):
        return False

    def clean_up(self):
        pass
