# -*- coding: utf8 -*-
from os.path import join, basename, exists, abspath
from nagini.loader import load_module, remove_ext, find_py
from nagini.builder.wrappers import FlowWrapper
from nagini import BaseFlow, EmbeddedFlow
from copy import deepcopy
from os import remove
import tempfile
import inspect
import shutil
import yaml
import sys


class PlainProjectPackage(object):
    base_dir = None
    _clean = False
    zip_path = None

    def __init__(self, project_path):
        self.project_path = project_path
        self.name = basename(abspath(project_path))
        self.tmp_dir = tempfile.mkdtemp(prefix=self.name + '-')

    def build(self, zip_filename=None, config=None):
        self.base_dir = self.tmp_dir
        shutil.copytree(self.project_path, join(self.tmp_dir, self.name))

        if zip_filename:
            self.zip_path = zip_filename
        else:
            _, self.zip_path = tempfile.mkstemp('.zip', self.name + '-')

        shutil.make_archive(
            remove_ext(self.zip_path),
            'zip',
            join(self.base_dir)
        )

        sys.stdout.write('{name:<30}{progress:>30}'.format(name=self.name, progress='OK'))
        sys.stdout.flush()

    def clear(self):
        if not self._clean:
            if exists(self.tmp_dir):
                shutil.rmtree(self.tmp_dir)
            if self.zip_path and exists(self.zip_path):
                remove(self.zip_path)
            self._clean = True


class ProjectPackage(object):
    base_dir = None
    _clean = False
    zip_path = None

    def __init__(self, project_path):
        self.project_path = project_path
        self.name = basename(abspath(project_path))
        self.tmp_dir = tempfile.mkdtemp(prefix=self.name + '-')
        self.jobs = {}

    def build(self, zip_filename=None, config=None):
        sys.path.insert(0, self.tmp_dir)
        self.base_dir = self.tmp_dir
        shutil.copytree(self.project_path, join(self.tmp_dir, self.name))
        for module_path, item in self._find_flows():
            wrapper = FlowWrapper(item, self)
            wrapper.build()

        if exists(join(self.project_path, 'system.properties')):
            shutil.copy(join(self.project_path, 'system.properties'),
                        self.base_dir)

        if zip_filename:
            self.zip_path = zip_filename
        else:
            _, self.zip_path = tempfile.mkstemp('.zip', self.name + '-')

        if config:
            config = deepcopy(config)
            config['project'] = self.name
            with open(join(self.base_dir, 'config.yml'), 'w') as fd:
                yaml.dump(config, fd)

        shutil.make_archive(
            remove_ext(self.zip_path),
            'zip',
            join(self.base_dir)
        )

    def _find_flows(self):
        modules = list(find_py(self.tmp_dir))
        for n, module_path in enumerate(modules):
            self.draw_progress(int(100.0 / len(modules) * (n + 1)))
            if module_path.startswith(self.tmp_dir):
                module_path = module_path.replace(self.tmp_dir, '', 1)
                module_path = module_path.strip('/')

            for name, item in inspect.getmembers(load_module(module_path)):
                if inspect.isclass(item) and issubclass(item, BaseFlow):
                    if not issubclass(item, EmbeddedFlow):
                        yield module_path, item
        print

    def draw_progress(self, progress):
        """Width: 60"""
        if progress == 100:
            bar = ''
            progress = 'OK'
        else:
            bar = '[{0:<18}]'.format('#' * int(18.0 / 100 * progress))
            progress = "%d%%" % progress

        sys.stdout.write(
            '{name:<30}     {bar:>20}{progress:>5}\r'.format(name=self.name,
                                                             bar=bar,
                                                             progress=progress)
        )
        sys.stdout.flush()

    def clear(self):
        if not self._clean:
            if exists(self.tmp_dir):
                shutil.rmtree(self.tmp_dir)
            if self.zip_path and exists(self.zip_path):
                remove(self.zip_path)
            self._clean = True

