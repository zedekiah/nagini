# -*- coding: utf8 -*-
from __future__ import absolute_import, division, print_function, unicode_literals

import json
import logging
import shutil
import sys
from abc import ABCMeta, abstractmethod
from copy import deepcopy
from datetime import datetime
from dateutil.relativedelta import relativedelta
from dateutil.rrule import DAILY, MO, MONTHLY, rrule, WEEKLY
from os import environ, mkdir
from os.path import exists, join

import yaml
from six import iteritems, itervalues, reraise

from nagini.fields import BaseField
from nagini.properties import props
from nagini.target import Target
from nagini.utility import flatten


class MetaForJobWithFields(ABCMeta):
    def __new__(mcs, name, bases, namespace):
        fields = set([])
        for base in bases:
            fields.update(base.__dict__.get('_nagini_fields', []))

        for field_name, field in iteritems(namespace):
            if isinstance(field, BaseField):
                if field.name is None:
                    field.name = field_name
                fields.add(field)
        namespace['_nagini_fields'] = fields
        return super(MetaForJobWithFields, mcs).__new__(mcs, name,
                                                        bases, namespace)


class JsonPropsFormatter(object):
    def __init__(self, props):
        self.props = props

    def render(self):
        return 'Init props:\n%s' % json.dumps(self.props, ensure_ascii=False, indent=4, sort_keys=True)


class BaseJob(object):
    __metaclass__ = MetaForJobWithFields
    _check_output_at_start = False
    _check_output_at_end = False
    props_formatter_class = JsonPropsFormatter

    name = None
    retries = 0
    retry_backoff = 0
    config = None

    def __init__(self):
        if not environ.get('NAGINI_BUILDING'):
            for field in getattr(self, '_nagini_fields', []):
                field.set_default_if_not_exists()

        self.env = environ.copy()
        self.logger = logging.getLogger('nagini.job.%s' %
                                        self.__class__.__name__)
        if 'working.dir' in props:
            props['working.dir.nagini'] = join(props['working.dir'],
                                               'nagini_data')

            with open(join(props['working.dir'], 'config.yml')) as fd:
                self.config = yaml.load(fd)

    def get_props_formatter(self):
        return self.props_formatter_class(self.props)

    def requires(self):
        """Override me!"""
        return []

    def output(self):
        """Override me!"""
        return []

    def input(self):
        """Return outputs of requires

        :rtype: Target|list[Target]|dict[str,Target]
        """
        requires = self.requires()
        if isinstance(requires, (tuple, list, set)):
            for job in requires:
                job.configure()
            outputs = [r.output() for r in requires]
        elif isinstance(requires, BaseJob):
            requires.configure()
            outputs = requires.output()
        elif isinstance(requires, dict):
            for item in itervalues(requires):
                item.configure()
            outputs = {k: v.output() for k, v in iteritems(requires)}
        else:
            raise ValueError('requires() must return BaseJob, list[BaseJob] '
                             'or dict[str, BaseJob]')

        # Set output_flag
        for output in flatten(outputs):
            output.output_flag = True
        return outputs

    def is_complete(self):
        return False

    def run(self):
        """Override me!"""
        self.logger.error('Nagini: this job is empty. Override run() method')

    def on_failure(self):
        for target in flatten(self.output()):
            if target.exists():
                target.clean_up()

    def on_success(self):
        pass

    def execute(self):
        props['working.dir.nagini'] = join(props['working.dir'], 'nagini_data')
        # if not exists(self.props["working.dir.nagini"]):
        try:
            mkdir(props['working.dir.nagini'])
        except OSError:
            pass
        self.configure()
        self.logger.info(self.get_props_formatter().render())
        output = flatten(self.output())
        if not output:
            self._check_output_at_start = False
        try:
            self.logger.info('Nagini: start job')
            if self._check_output_at_start and all(t.exists() for t in output):
                self.logger.warning('All targets exists at start of the job, skip job...')
            else:
                self.logger.info('Nagini: about to execute "run" method')
                self.run()
            for key, value in iteritems(self.env):
                props['env.%s' % key] = value

            if self._check_output_at_end and not all(t.exists() for t in output):
                raise Exception('Not all output target exists at end of the job')
            else:
                self.on_success()
        except:
            self.logger.exception('NaginiJob: catch exception. Try on_failure()')
            self.on_failure()
            reraise(*sys.exc_info())

    def rupdate_props(self, other):
        """Like self.props.update but not override existing props"""
        props.update_not_override(other)

    @staticmethod
    def data_path(path=''):
        return join(props['working.dir.nagini'], path)

    @staticmethod
    def clear_data_dir():
        if exists(props['working.dir.nagini']):
            shutil.rmtree(props['working.dir.nagini'])

    def configure(self):
        """Additional method to configure instead __init__
        Don't use __init__ to configure
        """

    @property
    def props(self):
        return props


class ClearAllJob(BaseJob):
    def run(self):
        self.clear_data_dir()


class BaseChecker(BaseJob):
    work_flow = None
    work_flow_params = None
    __metaclass__ = ABCMeta

    def run(self):
        if self.need_update():
            self.start_work_flow()

    @abstractmethod
    def need_update(self):
        raise NotImplementedError('You must implement "need_update" method')

    def start_work_flow(self):
        if self.work_flow is None:
            raise ValueError('You must set "work_flow" property')
        else:
            self.work_flow.start(self.work_flow_params or {})


class DataChecker(BaseChecker):
    __metaclass__ = ABCMeta

    def need_update(self):
        if self.src_data_exists() and self.dst_data_exists():
            self.start_work_flow()

    @abstractmethod
    def src_data_exists(self):
        raise NotImplementedError('You must implement '
                                  '"src_data_exists" method')

    @abstractmethod
    def dst_data_exists(self):
        raise NotImplementedError('You must implement '
                                  '"dst_data_exists" method')


class IntervalDataChecker(BaseJob):
    __metaclass__ = ABCMeta
    check_interval = None
    type = None
    src = None
    source_data_peace_delta = None
    source_data_count = 1
    prepared_data_pattern = None
    work_flow = None
    work_flow_params = None

    def run(self):
        end = None
        start = None

        if self.type == MONTHLY:
            end = datetime.now() - relativedelta(months=1, day=1, hour=0,
                                                 minute=0, second=0)
            start = end - relativedelta(months=self.check_interval)
        elif self.type == WEEKLY:
            end = datetime.now() - relativedelta(weekday=MO(-1), hour=0,
                                                 minute=0, second=0)
            start = end - relativedelta(weeks=self.check_interval)
        elif self.type == DAILY:
            end = datetime.now() - relativedelta(hour=0, minute=0, second=0)
            start = end - relativedelta(days=self.check_interval)

        for current in rrule(self.type, start, until=end):
            s = current
            if self.type == MONTHLY:
                e = s + relativedelta(months=1)
            elif self.type == WEEKLY:
                e = s + relativedelta(weeks=1)
            elif self.type == DAILY:
                e = s + relativedelta(days=1)

            if self.work_flow and not self.dst_data_exists(s, e):
                print('Prepared data not exists for', s, e)
                if self.src_data_exists(s, e):
                    print('Start prepare data. Start: %s, End: %s' % (s, e))
                    self.start_work_flow(s, e)

    @abstractmethod
    def src_data_exists(self, s, e):
        raise NotImplementedError('You must implement '
                                  '"src_data_exists" method')

    @abstractmethod
    def dst_data_exists(self, s, e):
        raise NotImplementedError('You must implement '
                                  '"dst_data_exists" method')

    def start_work_flow(self, s, e):
        params = deepcopy(self.work_flow_params) or {}

        if self.type == MONTHLY:
            params['month'] = s.strftime('%Y-%m')
        elif self.type == WEEKLY:
            params.update({'week': s.strftime('%Y-%m-%d'),
                           'start': s.strftime('%Y-%m-%d'),
                           'end': e.strftime('%Y-%m-%d')})
        elif self.type == DAILY:
            params['day'] = s.strftime('%Y-%m-%d')

        self.work_flow.start(params)
