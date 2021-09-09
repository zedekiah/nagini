# -*- coding: utf8 -*-
from __future__ import absolute_import, print_function, unicode_literals

import abc
import os
from subprocess import check_output


class Target(object):
    __metaclass__ = abc.ABCMeta
    output_flag = False

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

    def record_count(self):
        lines = check_output(['wc', '-l', self.path])
        return int(lines.split()[0])


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
