# -*- coding: utf8 -*-
from distutils.core import setup
from distutils.cmd import Command


class TestCommand(Command):
    user_options = []

    def initialize_options(self):
        pass

    def finalize_options(self):
        pass

    def run(self):
        pass


setup(
    name='nagini',
    version='0.21',
    author='Alexandr Litovchenko',
    author_email='zedlaa@gmail.com',
    packages=['nagini', 'nagini.builder'],
    scripts=['nagini-build.py', 'nagini-run.py', 'nagini-data.py'],
    requires=['jinja2', 'requests', 'PyYAML'],
    data_files=[
        ('/usr/share/nagini/', ['data/job-template.job.j2']),
        ('/usr/share/nagini/', ['data/launcher.py.j2']),
        ('/etc/', ['nagini.yml'])
    ],
    url='unknown',
    description='',
    cmdclass={
        'test': TestCommand
    }
)
