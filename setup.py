# -*- coding: utf8 -*-
from distutils.core import setup
from distutils.cmd import Command
import pytest


class TestCommand(Command):
    user_options = []

    def initialize_options(self):
        pass

    def finalize_options(self):
        pass

    def run(self):
        pytest.main()


setup(
    name='nagini',
    version='0.27',
    author='Alexandr Litovchenko',
    author_email='zedlaa@gmail.com',
    packages=['nagini',
              'nagini.builder',
              'nagini.contrib'],
    scripts=['nagini-build.py', 'nagini-run.py', 'nagini-data.py'],
    requires=['jinja2', 'requests', 'PyYAML'],
    data_files=[
        ('/usr/share/nagini/', ['data/job-template.job.j2']),
        ('/usr/share/nagini/', ['data/launcher.py.j2']),
        ('/etc/', ['config/nagini.yml'])
    ],
    url='https://github.com/zedekiah/nagini',
    description='',
    cmdclass={
        'test': TestCommand
    }
)
